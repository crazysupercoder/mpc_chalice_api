import uuid
from chalicelib.extensions import *
from chalicelib.utils.sqs_handlers.base import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.purchase.core import SimpleSku, Qty, Order
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
            'order_number': self.__order.number.value,
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
            customer = self.__customer_storage.get_by_id(order.customer_id)
            product = self.__product_storage.load(simple_sku)
            message = Message(
                str(uuid.uuid4()),
                customer.email.value,
                'Refund for Order #{}'.format(order.number.value),
                '"{}" has been Refunded in Qty {} for Order #{}'.format(
                    product.name.value,
                    qty.value,
                    order.number.value
                ),
            )
            self.__messages_storage.save(message)
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))

        __log_flow('End')


# ----------------------------------------------------------------------------------------------------------------------


class OrderPaymentOhHoldHandler(SqsHandlerInterface):
    def __init__(self):
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

        __log_flow('Start : {}'.format(sqs_message.message_data))

        if sqs_message.message_type != 'fixel_order_on_hold_by_portal':
            raise ValueError('{} does not know how to handle {} sqs message! Message data: {}'.format(
                self.__class__.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

        order_number_value = sqs_message.message_data.get('order_number')
        on_hold_status = sqs_message.message_data.get('status')

        order_number = Order.Number(order_number_value)
        order = self.__order_storage.load(order_number)

        if on_hold_status == Order.Status.CLOSED:
            self.__close_order_on_hold(order, __log_flow)
        else:
            self.__on_hold_not_closed_status(order, on_hold_status, __log_flow)

        self.__send_order_change_to_portal(order, __log_flow)
        self.__notify_about_order_status_change_silently(order, __log_flow)

        __log_flow('End')

    def __on_hold_not_closed_status(self, order: Order, on_hold_status: str, __log_flow) -> None:
        __log_flow('Order Updating...')
        order.status = Order.Status(on_hold_status)
        __log_flow('Order Updated!')

        __log_flow('Order Saving...')
        self.__order_storage.save(order)
        __log_flow('Order Saved!')

    def __close_order_on_hold(self, order: Order, __log_flow) -> None:
        __log_flow('Updating...')

        # close order
        __log_flow('Order Updating...')
        order.status = Order.Status(Order.Status.CLOSED)
        __log_flow('Order Updated!')

        # restore products qty
        __log_flow('Product Qty Updating - Start')
        products_to_save = []
        for order_item in order.items:
            if order_item.qty_processable.value == 0:
                __log_flow('Product Qty Updating: {} skipped because of 0 qty'.format(order_item.simple_sku.value))
                continue

            __log_flow('Product Qty Updating {} / {} ...'.format(
                order_item.simple_sku.value,
                order_item.qty_processable.value
            ))

            product = self.__products_storage.load(order_item.simple_sku)
            product.restore_qty(order_item.qty_processable)
            products_to_save.append(product)

            __log_flow('Product Qty Updated {} / {}!'.format(
                order_item.simple_sku.value,
                order_item.qty_processable.value
            ))

        __log_flow('Product Qty Updating - End')

        __log_flow('Updated!')

        __log_flow('Saving...')

        __log_flow('Order Saving...')
        self.__order_storage.save(order)
        __log_flow('Order Saved!')

        __log_flow('Products Saving...')
        for product in products_to_save:
            __log_flow('Product {} Saving...'.format(product.simple_sku.value))
            self.__products_storage.update(product)
            __log_flow('Product {} Saved!'.format(product.simple_sku.value))
        __log_flow('Products Saved!')

        __log_flow('Saved!')

    def __send_order_change_to_portal(self, order: Order, __log_flow) -> None:
        __log_flow('Order SQS: Sending...')
        self.__sqs_sender.send(OrderChangeSqsSenderEvent(order))
        __log_flow('Order SQS: Sent!')

    def __notify_about_order_status_change_silently(self, order: Order, __log_flow) -> None:
        try:
            __log_flow('Notification popup: Adding...')
            customer = self.__customer_storage.get_by_id(order.customer_id)
            self.__message_storage.save(Message(
                str(uuid.uuid4()),
                customer.email.value,
                'Order #{} status is changed!',
                'Order #{} has been turned to "{}" status!'.format(order.number.value, order.status.label)
            ))
            __log_flow('Notification popup: Added!')
        except BaseException as e:
            self.__logger.log_exception(e)
            __log_flow('Notification popup: Not Added because of Error : {}'.format(str(e)))


# ----------------------------------------------------------------------------------------------------------------------

