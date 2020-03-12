from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.purchase.core import Order, Id
from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.payment_methods.customer_credits import CustomerCreditsOrderPaymentMethod
from chalicelib.libs.purchase.order.service import OrderAppService
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.cart.service import CartAppService
from chalicelib.libs.purchase.checkout.service import CheckoutAppService


def register_payment_methods_customer_credits(blueprint: Blueprint) -> None:
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CHECKOUT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/customer-credits/checkout', methods=['POST'], cors=True)
    def customer_credits_checkout():
        checkout_storage = CheckoutStorageImplementation()
        order_storage = OrderStorageImplementation()
        order_app_service = OrderAppService()
        cart_service = CartAppService()
        checkout_service = CheckoutAppService()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        try:
            user = __get_user()

            # @todo : refactoring
            checkout = checkout_storage.load(Id(user.id))
            if not checkout:
                raise ApplicationLogicException('Checkout does not exist!')
            elif checkout.total_due.value != 0:
                raise ApplicationLogicException('Unable to checkout not 0 amount with Customer Credits!')

            order = order_app_service.get_waiting_for_payment_by_checkout_or_checkout_new(user.id)

            def __log_flow(text: str) -> None:
                logger.log_simple('Customer Credits Payment Log for Order #{} : {}'.format(
                    order.number.value,
                    text
                ))

            __log_flow('Start')

            try:
                __log_flow('Credits Spending...')
                # Attention!
                # Currently we use f-bucks only! Other credits are not available for now!
                # @todo : other credit types
                # @todo : copy-paste code
                # @todo : when reservation of credits amount will be done, perhaps, use sqs to spend credits
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
                    'script': 'ctx._source.amount -= ' + str(order.credit_spent_amount.value)
                })
                fbucks_customer_amount_changes_elastic.create(str(uuid.uuid4()) + str(order.customer_id.value), {
                    "customer_id": order.customer_id.value,
                    "amount": -order.credit_spent_amount.value,
                    "changed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "order_number": order.number.value,
                })
                __log_flow('Credits Spent!')

                __log_flow('Order Updating...')
                order.payment_method = CustomerCreditsOrderPaymentMethod()
                order.status = Order.Status(Order.Status.PAYMENT_SENT)
                order.status = Order.Status(Order.Status.PAYMENT_RECEIVED)
                order_storage.save(order)
                __log_flow('Order Updated!')
            except BaseException as e:
                __log_flow('Not done because of Error : {}'.format(str(e)))
                raise e

            # send order update to sqs
            try:
                __log_flow('Order Change SQS Sending...')
                sqs_sender.send(OrderChangeSqsSenderEvent(order))
                __log_flow('Order Change SQS Sent!')
            except BaseException as e:
                __log_flow('Order Change SQS NOT Sent because of Error: {}!'.format(str(e)))
                logger.log_exception(e)

            # flush cart
            try:
                __log_flow('Cart Flushing...')
                cart_service.clear_cart(user.session_id)
                __log_flow('Cart Flushed!')
            except BaseException as e:
                __log_flow('Cart NOT Flushed because of Error: {}'.format(str(e)))
                logger.log_exception(e)

            # flush checkout
            try:
                __log_flow('Checkout Flushing...')
                checkout_service.remove(user.id)
                __log_flow('Checkout Flushed!')
            except BaseException as e:
                __log_flow('Checkout NOT Flushed because of Error: {}'.format(str(e)))
                logger.log_exception(e)

            result = {
                'order_number': order.number.value
            }

            __log_flow('End')

            return result
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

