import os
import uuid
import hashlib
from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.core.file_storage import FileStorageImplementation
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.core.mailer import MailerImplementation
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.purchase.core import Order, Id
from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.payment_methods.regular_eft.payment import RegularEftOrderPaymentMethod
from chalicelib.libs.purchase.payment_methods.regular_eft.sqs import RegularEftProofUploadedSqsSenderEvent
from chalicelib.libs.purchase.payment_methods.regular_eft.mail import RegularEftBankDetailsMailMessage
from chalicelib.libs.purchase.order.service import OrderAppService
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent


def register_payment_methods_regular_eft(blueprint: Blueprint) -> None:
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CHECKOUT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/regular-eft/checkout', methods=['POST'], cors=True)
    def regular_eft_checkout():
        checkout_storage = CheckoutStorageImplementation()
        order_storage = OrderStorageImplementation()
        order_app_service = OrderAppService()
        logger = Logger()
        mailer = MailerImplementation()

        # 1. Get or create order. Critical!
        # ------------------------------------------------------

        try:
            user = __get_user()

            # @todo : refactoring
            checkout = checkout_storage.load(Id(user.id))
            if not checkout:
                raise ApplicationLogicException('Checkout does not exist!')
            elif checkout.total_due.value == 0:
                raise ApplicationLogicException('Unable to checkout 0 amount with Regular Eft!')

            order = order_app_service.get_waiting_for_payment_by_checkout_or_checkout_new(user.id)

            def __log_order_flow(text: str) -> None:
                logger.log_simple('Regular EFT : Checkout : {} : {}'.format(order.number.value, text))

            __log_order_flow('Start')

            # Attention!
            # Currently we use f-bucks only! Other credits are not available for now!
            # @todo : other credit types
            # @todo : copy-paste code
            # @todo : when reservation of credits amount will be done, perhaps, use sqs to spend credits
            if order.credit_spent_amount.value > 0:
                __log_order_flow('Spending Credits...')
                """"""
                from chalicelib.libs.purchase.core import Checkout
                see = Checkout.__init__
                """"""
                # @TODO : refactoring : raw data usage
                import uuid
                import datetime
                from chalicelib.settings import settings
                from chalicelib.libs.core.elastic import Elastic
                fbucks_customer_amount_elastic = Elastic(
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                )
                fbucks_customer_amount_changes_elastic = Elastic(
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
                )
                fbucks_customer_amount_elastic.update_data(order.customer_id.value, {
                    'script': 'ctx._source.amount -= ' + str(order.credit_spent_amount.value),
                })
                fbucks_customer_amount_changes_elastic.create(str(uuid.uuid4()) + str(order.customer_id.value), {
                    "customer_id": order.customer_id.value,
                    "amount": -order.credit_spent_amount.value,
                    "changed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "order_number": order.number.value,
                })
                __log_order_flow('Spending Credits: Done!')

            __log_order_flow('Order Updating...')
            order.payment_method = RegularEftOrderPaymentMethod()
            order_storage.save(order)
            __log_order_flow('Order Updated!')
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

        # 2. Send eft email. Not critical.
        # Theoretically can be redone or downloaded manually.
        # ------------------------------------------------------

        try:
            __log_order_flow('EFT Email Sending...')
            message = RegularEftBankDetailsMailMessage(order)
            mailer.send(message)
            __log_order_flow('EFT Email Sent!')
        except BaseException as e:
            logger.log_exception(e)
            __log_order_flow('EFT Email Not Sent because of Error: {}'.format(str(e)))

        # 3. Flush cart, checkout. Not critical.
        # ------------------------------------------------------

        # flush cart
        try:
            __log_order_flow('Cart Flushing...')
            from chalicelib.libs.purchase.cart.service import CartAppService
            cart_service = CartAppService()
            cart_service.clear_cart(user.session_id)
            __log_order_flow('Cart Flushed!')
        except BaseException as e:
            logger.log_exception(e)
            __log_order_flow('Cart Not Flushed because of Error: {}'.format(str(e)))

        # flush checkout
        try:
            __log_order_flow('Checkout Flushing...')
            from chalicelib.libs.purchase.checkout.service import CheckoutAppService
            checkout_service = CheckoutAppService()
            checkout_service.remove(user.id)
            __log_order_flow('Checkout Flushed!')
        except BaseException as e:
            logger.log_exception(e)
            __log_order_flow('Checkout Not Flushed because of Error: {}'.format(str(e)))

        return {
            'order_number': order.number.value
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   UPLOAD
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route(
        '/payment-methods/regular-eft/payment-proof/upload',
        methods=['POST'], cors=True, content_types=['application/octet-stream']
    )
    def regular_eft_payment_proof_upload():
        file_storage = FileStorageImplementation()

        try:
            user_id = __get_user().id

            # @todo : create file uploader or so... ???

            max_size_mb = 4
            size_in_bytes = len(blueprint.current_request.raw_body)
            if size_in_bytes > max_size_mb * 1024 * 1024:
                raise HttpIncorrectInputDataException('Uploaded file max size is {} Mb!'.format(max_size_mb))
            elif not size_in_bytes:
                raise HttpIncorrectInputDataException('Uploaded file cannot be empty!')

            file_id = str(user_id) + str(uuid.uuid4())
            file_id = hashlib.md5(file_id.encode('utf-8')).hexdigest()
            file_content = blueprint.current_request.raw_body

            # save tmp file
            tmp_file_path = '/tmp/' + file_id
            with open(tmp_file_path, 'wb') as tmp_file:
                tmp_file.write(file_content)

            # check tmp file
            import fleep
            with open(tmp_file_path, 'rb') as tmp_file:
                file_info = fleep.get(tmp_file.read(128))

            types_map = {
                'image/png': 'png',
                'image/jpeg': 'jpg',
                'image/pjpeg': 'jpg',
                'image/bmp': 'bmp',
                'image/x-windows-bmp': 'bmp',
                'image/gif': 'gif',
                'application/pdf': 'pdf',
            }
            if not file_info.mime or file_info.mime[0] not in types_map.keys():
                raise HttpIncorrectInputDataException('Mime-type is not supported!')

            # upload
            extension = types_map[file_info.mime[0]]
            destination_key = file_id + '.' + extension
            file_storage.upload(tmp_file_path, destination_key)

            # remove tmp file
            if os.path.isfile(tmp_file_path):
                os.remove(tmp_file_path)

            return {
                'key': destination_key,
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                           PAYMENT PROOF - SEND
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/regular-eft/payment-proof/send', methods=['PUT'], cors=True)
    def regular_eft_payment_proof_send():
        order_storage = OrderStorageImplementation()
        logger = Logger()
        file_storage = FileStorageImplementation()
        sqs_sender = SqsSenderImplementation()

        try:
            user_id = __get_user().id

            request_data = blueprint.current_request.json_body
            order_number_value = str(request_data.get('order_number', '')).strip()
            file_id = str(request_data.get('file_id', '')).strip()
            if not order_number_value:
                raise HttpIncorrectInputDataException('order_number is required!')
            elif not file_id:
                raise HttpIncorrectInputDataException('file_id is required!')

            order_number = Order.Number(order_number_value)
            order = order_storage.load(order_number)
            if not order:
                raise HttpNotFoundException('Order "{}" does not exist!'.format(order_number))
            elif order.customer_id.value != user_id:
                raise HttpAccessDenyException('Access Denied!')
            elif not isinstance(order.payment_method, RegularEftOrderPaymentMethod):
                raise ApplicationLogicException('Order "{}" does not have Regular EFT payment!')

            file = file_storage.get(file_id)
            if not file:
                raise HttpNotFoundException('File does not exist!')

            def __log_flow(text: str) -> None:
                logger.log_simple('Regular EFT : Sending POP : {} : {}'.format(order.number.value, text))

            __log_flow('Order Updating...')
            order.status = Order.Status(Order.Status.PAYMENT_SENT)
            order_storage.save(order)
            __log_flow('Order Updated!')

            try:
                __log_flow('Order SQS Sending...')
                sqs_sender.send(OrderChangeSqsSenderEvent(order))
                __log_flow('Order SQS Sent!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Order SQS NOT Sent because of Error: {}!'.format(str(e)))

            try:
                __log_flow('Regular EFT POP SQS Sending...')
                sqs_sender.send(RegularEftProofUploadedSqsSenderEvent(order, file))
                __log_flow('Regular EFT POP SQS Sent!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Regular EFT POP SQS NOT Sent because of Error: {}!'.format(str(e)))

            return {
                'Code': 'Success',
                'Message': 'Success',
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
