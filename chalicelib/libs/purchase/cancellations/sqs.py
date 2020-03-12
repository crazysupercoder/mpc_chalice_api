import uuid
from chalicelib.extensions import *
from chalicelib.utils.sqs_handlers.base import SqsMessage, SqsHandlerInterface
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.purchase.core import SimpleSku, Qty, Order, ProductInterface, CancelRequest
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.purchase.cancellations.storage import CancelRequestStorageImplementation
from chalicelib.libs.message.base import Message, MessageStorageImplementation


# ----------------------------------------------------------------------------------------------------------------------


class CancelRequestPaidOrderSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, cancel_request: CancelRequest) -> None:
        if not isinstance(cancel_request, CancelRequest):
            raise ArgumentTypeException(self.__init__, 'cancel_request', cancel_request)

        self.__cancel_request = cancel_request

    @classmethod
    def _get_event_type(cls) -> str:
        return 'fixel_paid_order_cancellation_request'

    @property
    def event_data(self) -> dict:
        request = self.__cancel_request

        return {
            'request_number': request.number.value,
            'requested_at': request.requested_at.strftime('%Y-%m-%d %H:%M:%S'),
            'order_number': request.order_number.value,
            'items': [{
                'simple_sku': item.simple_sku.value,
                'qty': item.qty.value,
            } for item in request.items],
            'refund_method': {
                'descriptor': request.refund_method.descriptor,
                'label': request.refund_method.label,
                'extra_data': request.refund_method.extra_data,
            },
            'additional_comment': request.additional_comment.value if request.additional_comment else None
        }


# ----------------------------------------------------------------------------------------------------------------------


class CancelRequestPaidOrderAnswerSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__orders_storage = OrderStorageImplementation()
        self.__products_storage = ProductStorageImplementation()
        self.__cancel_request_storage = CancelRequestStorageImplementation()
        self.__customer_storage = CustomerStorageImplementation()
        self.__messages_storage = MessageStorageImplementation()
        self.__sqs_sender = SqsSenderImplementation()
        self.__logger = Logger()

    def handle(self, sqs_message: SqsMessage) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : {} : {}'.format(
                self.__class__.__qualname__,
                sqs_message.id,
                text
            ))

        __log_flow('Start - {}'.format(sqs_message.message_data))

        request_number = CancelRequest.Number(sqs_message.message_data['request_number'])
        order_number = Order.Number(sqs_message.message_data['order_number'])
        simple_sku = SimpleSku(sqs_message.message_data['simple_sku'])
        qty = Qty(sqs_message.message_data['qty'])
        status = sqs_message.message_data['status']

        actions_map = {
            'approved': self.__approve,
            'declined': self.__decline,
        }

        action = actions_map.get(status)
        if not action:
            raise Exception('{} can\'t handle SQS message {}:{}! Status is unknown!'.format(
                self.handle.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

        action(request_number, order_number, simple_sku, qty, __log_flow)

        __log_flow('End')

    def __approve(
        self,
        request_number: CancelRequest.Number,
        order_number: Order.Number,
        simple_sku: SimpleSku,
        qty: Qty,
        __log_flow
    ) -> None:
        __log_flow('Approving...')

        cancel_request = self.__cancel_request_storage.get_by_number(request_number)
        order = self.__orders_storage.load(order_number)
        product = self.__products_storage.load(simple_sku)

        cancel_request.approve_item(simple_sku)
        order.approve_cancellation_after_payment(simple_sku, qty)
        product.restore_qty(qty)
        order_change_event = OrderChangeSqsSenderEvent(order)

        __log_flow('Cancel Request Saving...')
        self.__cancel_request_storage.save(cancel_request)
        __log_flow('Cancel Request Saved!')

        __log_flow('Order Saving...')
        self.__orders_storage.save(order)
        __log_flow('Order Saved!')

        __log_flow('Product Saving...')
        self.__products_storage.update(product)
        __log_flow('Product Saved!')

        __log_flow('Order SQS Sending...')
        self.__sqs_sender.send(order_change_event)
        __log_flow('Order SQS Sent!')

        try:
            __log_flow('Notification popup: Adding...')
            self.__add_notification_message(cancel_request, order, product, 'Approved')
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('Approved!')

    def __decline(
        self,
        request_number: CancelRequest.Number,
        order_number: Order.Number,
        simple_sku: SimpleSku,
        qty: Qty,
        __log_flow
    ) -> None:
        __log_flow('Declining...')

        cancel_request = self.__cancel_request_storage.get_by_number(request_number)
        order = self.__orders_storage.load(order_number)
        product = self.__products_storage.load(simple_sku)

        cancel_request.decline_item(simple_sku)
        order.decline_cancellation_after_payment(simple_sku, qty)
        order_change_event = OrderChangeSqsSenderEvent(order)

        __log_flow('Cancel Request Saving...')
        self.__cancel_request_storage.save(cancel_request)
        __log_flow('Cancel Request Saved!')

        __log_flow('Order Saving...')
        self.__orders_storage.save(order)
        __log_flow('Order Saved!')

        __log_flow('Order SQS Sending...')
        self.__sqs_sender.send(order_change_event)
        __log_flow('Order SQS Sent!')

        try:
            __log_flow('Notification popup: Adding...')
            self.__add_notification_message(cancel_request, order, product, 'Declined')
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('Declined!')

    def __add_notification_message(
        self,
        cancel_request: CancelRequest,
        order: Order,
        product: ProductInterface,
        status_label: str
    ) -> None:
        customer = self.__customer_storage.get_by_id(order.customer_id)
        message = Message(
            str(uuid.uuid4()),
            customer.email.value,
            'Cancellation Request #{} has been Updated!'.format(cancel_request.number.value),
            'Cancellation Request for Product "{}" for Order #{} has been {}!'.format(
                product.name.value,
                order.number.value,
                status_label
            ),
        )
        self.__messages_storage.save(message)


# ----------------------------------------------------------------------------------------------------------------------


class CancelledOrderOnPortalSideSqsHandle(SqsHandlerInterface):
    def __init__(self):
        self.__orders_storage = OrderStorageImplementation()
        self.__products_storage = ProductStorageImplementation()
        self.__cancel_request_storage = CancelRequestStorageImplementation()
        self.__customer_storage = CustomerStorageImplementation()
        self.__messages_storage = MessageStorageImplementation()
        self.__sqs_sender = SqsSenderImplementation()
        self.__logger = Logger()

    def handle(self, sqs_message: SqsMessage) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : {} : {}'.format(
                self.__class__.__qualname__,
                sqs_message.id,
                text
            ))

        __log_flow('Start - {}'.format(sqs_message.message_data))

        order_number = Order.Number(sqs_message.message_data['order_number'])
        simple_sku = SimpleSku(sqs_message.message_data['simple_sku'])
        qty = Qty(sqs_message.message_data['qty'])

        order = self.__orders_storage.load(order_number)
        product = self.__products_storage.load(simple_sku)

        if order.was_paid:
            order.request_cancellation_after_payment(simple_sku, qty)
            order.approve_cancellation_after_payment(simple_sku, qty)
        else:
            order.cancel_before_payment(simple_sku, qty)

        product.restore_qty(qty)
        order_change_event = OrderChangeSqsSenderEvent(order)

        __log_flow('Order Saving...')
        self.__orders_storage.save(order)
        __log_flow('Order Saved!')

        __log_flow('Product Saving...')
        self.__products_storage.update(product)
        __log_flow('Product Saved!')

        __log_flow('Order SQS Sending...')
        self.__sqs_sender.send(order_change_event)
        __log_flow('Order SQS Sent!')

        try:
            __log_flow('Notification popup: Adding...')
            customer = self.__customer_storage.get_by_id(order.customer_id)
            message = Message(
                str(uuid.uuid4()),
                customer.email.value,
                'Order #{} has been Updated!'.format(order.number.value),
                'Product "{}" for Order #{} has been Cancelled in Qty {}!'.format(
                    product.name.value,
                    order.number.value,
                    qty.value
                ),
            )
            self.__messages_storage.save(message)
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('End')


# ----------------------------------------------------------------------------------------------------------------------

