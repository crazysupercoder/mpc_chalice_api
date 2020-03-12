import uuid
from chalicelib.extensions import *
from chalicelib.libs.core.logger import Logger
from chalicelib.utils.sqs_handlers.base import SqsMessage, SqsHandlerInterface
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation, SqsSenderEventInterface
from chalicelib.libs.core.file_storage import FileStorageFile
from chalicelib.libs.purchase.core import Order
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.message.base import MessageStorageImplementation, Message
from chalicelib.libs.purchase.payment_methods.regular_eft.payment import RegularEftOrderPaymentMethod


# ----------------------------------------------------------------------------------------------------------------------


class RegularEftProofUploadedSqsSenderEvent(SqsSenderEventInterface):
    @classmethod
    def _get_event_type(cls) -> str:
        return 'eft_proof_uploaded'

    def __init__(self, order: Order, proof_file: FileStorageFile):
        if not isinstance(order, Order):
            raise ArgumentTypeException(self.__init__, 'order', order)

        if not isinstance(proof_file, FileStorageFile):
            raise ArgumentTypeException(self.__init__, 'proof_file', proof_file)

        self.__order = order
        self.__proof_file = proof_file

    @property
    def event_data(self) -> dict:
        order = self.__order
        proof_file = self.__proof_file

        data = {
            'order_number': order.number.value,
            'proof_file_url': proof_file.url,
        }

        return data


# ----------------------------------------------------------------------------------------------------------------------


class RegularEftPaymentSqsHandler(SqsHandlerInterface):
    def __init__(self):
        # Need to be imported here, because orders storage depends from payment method class in this file.
        from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
        from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
        self.__order_storage = OrderStorageImplementation()
        self.__sqs_sender = SqsSenderImplementation()
        self.__logger = Logger()
        self.__message_storage = MessageStorageImplementation()
        self.__customer_storage = CustomerStorageImplementation()
        self.__products_storage = ProductStorageImplementation()

    def handle(self, sqs_message: SqsMessage) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : SQS Message #{} : {}'.format(
                self.__class__.__qualname__,
                sqs_message.id,
                text
            ))

        __log_flow('Start')

        if sqs_message.message_type != 'regular_eft_proof_check_result':
            raise ValueError('{} does not know how to handle {} sqs message! Message data: {}'.format(
                self.__class__.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

        order_number_value = sqs_message.message_data.get('order_number')
        is_proof_accepted = sqs_message.message_data.get('is_proof_accepted')

        __log_flow('Order #{} - Payment - {}'.format(
            order_number_value,
            'Accepted' if is_proof_accepted else 'Declined'
        ))

        __log_flow('Updating...')
        order_number = Order.Number(order_number_value)
        order = self.__order_storage.load(order_number)
        if not order:
            raise ValueError('Unable to handle {} sqs-message #{}: order does not exist. Message data: {}'.format(
                sqs_message.message_type,
                sqs_message.id,
                sqs_message.message_data
            ))

        if not isinstance(order.payment_method, RegularEftOrderPaymentMethod):
            raise ValueError('Order #{} is not a Regular EFT payment order!'.format(order.number.value))

        if is_proof_accepted:
            # accept order payment
            __log_flow('Order Updating...')
            order.status = Order.Status(Order.Status.PAYMENT_RECEIVED)
            self.__order_storage.save(order)
            __log_flow('Order Updated!')
        else:
            # Attention!
            # Order must be closed first to avoid multiple "restore-qty" actions!
            # @todo : refactoring ?

            # close order
            __log_flow('Order Closing...')
            order.status = Order.Status(Order.Status.CLOSED)
            self.__order_storage.save(order)
            __log_flow('Order Closed!')

            # restore products qty
            __log_flow('Product Qty Restoring - Start')
            for order_item in order.items:
                if order_item.qty_processable.value == 0:
                    __log_flow('Product Qty Restoring: {} skipped because of 0 qty'.format(order_item.simple_sku.value))
                    continue

                try:
                    __log_flow('Product Qty Restoring {} / {} ...'.format(
                        order_item.simple_sku.value,
                        order_item.qty_processable.value
                    ))
                    product = self.__products_storage.load(order_item.simple_sku)
                    product.restore_qty(order_item.qty_processable)
                    self.__products_storage.update(product)
                    __log_flow('Product Qty Restored {} / {}!'.format(
                        order_item.simple_sku.value,
                        order_item.qty_processable.value
                    ))
                except BaseException as e:
                    self.__logger.log_exception(e)
                    __log_flow('Product Qty NOT Restored {} / {} because of Error: '.format(
                        order_item.simple_sku.value,
                        order_item.qty_processable.value,
                        str(e)
                    ))

            __log_flow('Product Qty Restoring - End')

        __log_flow('Updated!')

        # send to portal
        __log_flow('Order SQS: Sending...')
        self.__sqs_sender.send(OrderChangeSqsSenderEvent(order))
        __log_flow('Order SQS: Sent!')

        # silently add notification (silently)
        try:
            __log_flow('Notification popup: Adding...')
            customer = self.__customer_storage.get_by_id(order.customer_id)
            if not customer:
                raise ValueError('{} cant notify customer #{} about Regular-EFT payment updates for Order #{}'.format(
                    self.handle.__qualname__,
                    order.customer_id.value,
                    order.number.value
                ))

            self.__message_storage.save(Message(
                str(uuid.uuid4()),
                customer.email.value,
                'Regular EFT Payment has been checked!',
                'Regular EFT Payment for Order #{} has been checked and {}!'.format(
                    order.number.value,
                    'Accepted' if is_proof_accepted else 'Declined'
                )
            ))
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('End')


# ----------------------------------------------------------------------------------------------------------------------

