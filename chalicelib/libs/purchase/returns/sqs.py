import uuid
from chalicelib.extensions import *
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.purchase.core import OrderNumber, SimpleSku, ReturnRequest
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.purchase.returns.storage import ReturnRequestStorageImplementation
from chalicelib.libs.message.base import Message, MessageStorageImplementation
from chalicelib.utils.sqs_handlers.base import SqsMessage, SqsHandlerInterface


class ReturnRequestChangeSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, return_request: ReturnRequest) -> None:
        if not isinstance(return_request, ReturnRequest):
            raise ArgumentTypeException(self.__init__, 'return_request', return_request)

        self.__return_request = return_request

    @classmethod
    def _get_event_type(cls) -> str:
        return 'return_request_change'

    @property
    def event_data(self) -> dict:
        return {
            'return_request_number': self.__return_request.number.value,
        }


class ReturnRequestChangeSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__returns_storage = ReturnRequestStorageImplementation()
        self.__order_storage = OrderStorageImplementation()
        self.__customer_storage = CustomerStorageImplementation()
        self.__product_storage = ProductStorageImplementation()
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

        request_number = sqs_message.message_data.get('return_request_number')
        order_number = sqs_message.message_data.get('order_number')
        simple_sku = sqs_message.message_data.get('simple_sku')
        status = sqs_message.message_data.get('status')

        __log_flow('Updating...')
        return_request = self.__returns_storage.load(ReturnRequest.Number(request_number))
        if not return_request:
            raise ValueError('{} can\'t handle SQS message {}:{}! Return Request does not exist!'.format(
                self.handle.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

        actions_map = {
            'approved': self.__approve,
            'cancelled': self.__decline,
            'closed': self.__close,
        }

        action = actions_map.get(status)
        if not action:
            raise Exception('{} can\'t handle SQS message {}:{}! Status is unknown!'.format(
                self.handle.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

        action(return_request, OrderNumber(order_number), SimpleSku(simple_sku), __log_flow)
        __log_flow('Updated!')

        __log_flow('End')

    def __approve(
        self,
        return_request: ReturnRequest,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        __log_flow
    ) -> None:
        __log_flow('Approving...')

        return_request.make_item_approved(order_number, simple_sku)

        __log_flow('Return Request Saving...')
        self.__returns_storage.save(return_request)
        __log_flow('Return Request Saved!')

        try:
            __log_flow('Notification popup: Adding...')
            self.__add_notification_message(return_request, order_number, simple_sku, 'Approved')
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('Approved!')

    def __decline(
        self,
        return_request: ReturnRequest,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        __log_flow
    ) -> None:
        __log_flow('Declining...')

        qty = return_request.get_item_qty(order_number, simple_sku)
        order = self.__order_storage.load(order_number)

        # updating
        return_request.make_item_declined(order_number, simple_sku)
        order.decline_return(simple_sku, qty)

        # saving
        __log_flow('Declining: Return Request Saving...')
        self.__returns_storage.save(return_request)
        __log_flow('Declining: Return Request Saved!')

        __log_flow('Declining: Order Saving...')
        self.__order_storage.save(order)
        __log_flow('Declining: Order Saved!')

        # add notification silently
        try:
            __log_flow('Notification popup: Adding...')
            self.__add_notification_message(return_request, order_number, simple_sku, 'Declined')
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('Declined!')

    def __close(
        self,
        return_request: ReturnRequest,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        __log_flow
    ) -> None:
        __log_flow('Closing...')

        qty = return_request.get_item_qty(order_number, simple_sku)
        product = self.__product_storage.load(simple_sku)
        order = self.__order_storage.load(order_number)

        product.restore_qty(qty)
        order.close_return(simple_sku, qty)
        return_request.make_item_closed(order_number, simple_sku)

        # update product
        __log_flow('Closing: Product Qty: Restoring...')
        self.__product_storage.update(product)
        __log_flow('Closing: Product Qty: Restored!')

        # update order
        __log_flow('Closing: Order Qty: Returning...')
        self.__order_storage.save(order)
        __log_flow('Closing: Order Qty: Returned!')

        # update request
        __log_flow('Closing: Return Request: Updating...')
        self.__returns_storage.save(return_request)
        __log_flow('Closing: Return Request: Updated!')

        __log_flow('Closing: Order SQS: Sending...')
        self.__sqs_sender.send(OrderChangeSqsSenderEvent(order))
        __log_flow('Closing: Order SQS: Sent!')

        # add notification silently
        try:
            __log_flow('Notification popup: Adding...')
            self.__add_notification_message(return_request, order_number, simple_sku, 'Closed')
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('Closed!')

    def __add_notification_message(
        self,
        return_request: ReturnRequest,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        status_label: str
    ) -> None:
        order = self.__order_storage.load(order_number)
        customer = self.__customer_storage.get_by_id(order.customer_id)
        product = self.__product_storage.load(simple_sku)
        message = Message(
            str(uuid.uuid4()),
            customer.email.value,
            'Return Request #{} has been Updated!'.format(return_request.number.value),
            'Return Request for Product "{}" for Order #{} has been {}!'.format(
                product.name.value,
                order_number.value,
                status_label
            ),
        )
        self.__messages_storage.save(message)

