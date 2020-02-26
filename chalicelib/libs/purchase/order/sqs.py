import uuid
from chalicelib.extensions import *
from chalicelib.utils.sqs_handlers.base import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.purchase.core.values import SimpleSku, Qty
from chalicelib.libs.purchase.core.order import Order
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.message.base import Message, MessageStorageImplementation


# ----------------------------------------------------------------------------------------------------------------------


class OrderChangeSqsSenderEvent(SqsSenderEventInterface):
    @classmethod
    def _get_event_type(cls) -> str:
        return 'order_change'

    def __init__(self, order: Order):
        if not isinstance(order, Order):
            raise ArgumentTypeException(self.__init__, 'order', order)

        self.__order = order

    @property
    def event_data(self) -> dict:
        return {
            'order_number': self.__order.order_number.value,
        }


# ----------------------------------------------------------------------------------------------------------------------


class OrderChangeSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__messages_storage = MessageStorageImplementation()
        self.__order_storage = OrderStorageImplementation()
        self.__sqs_sender = SqsSenderImplementation()
        self.__logger = Logger()

    def handle(self, sqs_message: SqsMessage) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : SQS Message #{} : {}'.format(
                self.__class__.__qualname__,
                sqs_message.id,
                text
            ))

        __log_flow('Start - {}'.format(sqs_message.message_data))

        data = {
            'order_number': sqs_message.message_data.get('order_number', '') or '',
            'order_status_mpc': sqs_message.message_data.get('order_status_mpc', '') or '',
            'popup_message': {
                'customer_email': sqs_message.message_data.get('popup_message').get('customer_email'),
                'message_title': sqs_message.message_data.get('popup_message').get('message_title'),
                'message_text': sqs_message.message_data.get('popup_message').get('message_text'),
            } if sqs_message.message_data.get('popup_message', None) or None else None,
        }

        __log_flow('Order: Updating...')
        order_number = Order.Number(data.get('order_number'))
        order = self.__order_storage.load(order_number)
        if not order:
            raise ValueError('Order "{}" does not exist in the MPC!')

        mpc_order_status = str(data.get('order_status_mpc'))
        order.status = Order.Status(mpc_order_status)
        __log_flow('Order: Updated!')

        __log_flow('Order: Saving...')
        self.__order_storage.save(order)
        __log_flow('Order: Saved!')

        # Attention!
        # We need to send-back order changes because of compatibility reason.
        __log_flow('Order: SQS Sending-Back...')
        self.__sqs_sender.send(OrderChangeSqsSenderEvent(order))
        __log_flow('Order: SQS Sent-Back!')

        # add message, if is needed (silently)
        try:
            message_data = data.get('popup_message') or None
            if message_data:
                __log_flow('Notification popup: Adding...')
                message = Message(
                    str(uuid.uuid4()),
                    message_data.get('customer_id'),
                    message_data.get('message_title'),
                    message_data.get('message_text'),
                )
                self.__messages_storage.save(message)
                __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('End')


# ----------------------------------------------------------------------------------------------------------------------


class OrderRefundSqsHandler(SqsHandlerInterface):
    def __init__(self) -> None:
        self.__messages_storage = MessageStorageImplementation()
        self.__order_storage = OrderStorageImplementation()
        self.__customer_storage = CustomerStorageImplementation()
        self.__product_storage = ProductStorageImplementation()
        self.__sqs_sender = SqsSenderImplementation()
        self.__logger = Logger()

    def handle(self, sqs_message: SqsMessage) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : SQS Message #{} : {}'.format(
                self.__class__.__qualname__,
                sqs_message.id,
                text
            ))

        __log_flow('Start - {}'.format(sqs_message.message_data))

        order_number = Order.Number(sqs_message.message_data['order_number'])
        simple_sku = SimpleSku(sqs_message.message_data['simple_sku'])
        qty = Qty(sqs_message.message_data['qty'])

        __log_flow('Order Updating...')
        order = self.__order_storage.load(order_number)
        order.refund(simple_sku, qty)
        __log_flow('Order Updated!')

        __log_flow('Order Saving...')
        self.__order_storage.save(order)
        __log_flow('Order Saved!')

        __log_flow('Order SQS Sending...')
        self.__sqs_sender.send(OrderChangeSqsSenderEvent(order))
        __log_flow('Order SQS Sent!')

        # add message (silently)
        try:
            __log_flow('Notification popup: Adding...')
            customer = self.__customer_storage.load(order.customer_id)
            product = self.__product_storage.load(simple_sku)
            message = Message(
                str(uuid.uuid4()),
                customer.email.value,
                'Refund for Order #{}'.format(order.order_number.value),
                '"{}" has been Refunded in Qty {} for Order #{}'.format(
                    product.name.value,
                    qty.value,
                    order.order_number.value
                ),
            )
            self.__messages_storage.save(message)
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('End')


# ----------------------------------------------------------------------------------------------------------------------

