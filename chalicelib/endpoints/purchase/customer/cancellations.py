import datetime
from typing import Dict, Optional, Tuple
from chalice import Blueprint, \
    BadRequestError, \
    UnauthorizedError, \
    NotFoundError, \
    ForbiddenError, \
    UnprocessableEntityError
from chalicelib.extensions import *
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.purchase.core import SimpleSku, Qty, ProductInterface, Order, CancelRequest, RefundMethodAbstract
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.purchase.cancellations.storage import CancelRequestStorageImplementation
from chalicelib.libs.purchase.cancellations.sqs import CancelRequestPaidOrderSqsSenderEvent
from chalicelib.libs.purchase.payment_methods.refund_methods import StoreCreditRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import EftRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import CreditCardRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import MobicredRefundMethod


def register_customer_cancellations(blueprint: Blueprint):
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise UnauthorizedError('Authentication is required!')

        return user

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   GET
    # ------------------------------------------------------------------------------------------------------------------

    def __get_cancellation_requests_response(requests: Tuple[CancelRequest]):
        products_storage = ProductStorageImplementation()

        products_map: Dict[str, ProductInterface] = {}
        for request in requests:
            for item in request.items:
                product = products_map.get(item.simple_sku.value) or products_storage.load(item.simple_sku)
                products_map[item.simple_sku.value] = product

        result = [{
            'request_number': request.number.value,
            'order_number': request.order_number.value,
            'items': [{
                'simple_sku': item.simple_sku.value,
                'qty': item.qty.value,
                'status': {
                    'value': item.status.value,
                    'label': item.status.label,
                },
                'product_name': products_map[item.simple_sku.value].name.value,
                'img_url': (
                    products_map[item.simple_sku.value].image_urls[0]
                    if products_map[item.simple_sku.value].image_urls else None
                ),
            } for item in request.items],
            'status': {
                'value': request.total_status.value,
                'label': request.total_status.label,
            },
            'refund_method': {
                'descriptor': request.refund_method.descriptor,
                'label': request.refund_method.label,
            },
            'additional_comment': request.additional_comment.value if request.additional_comment else None
        } for request in requests]

        return result

    @blueprint.route('/customer/cancellations/get_all_by_order/{order_number}', methods=['GET'], cors=True)
    def cancellation_get_by_order(order_number):
        user = __get_user()
        orders_storage = OrderStorageImplementation()
        cancel_requests_storage = CancelRequestStorageImplementation()

        try:
            order_number = Order.Number(order_number)
        except BaseException:
            raise BadRequestError('Incorrect Input Data!')

        order = orders_storage.load(order_number)
        if not order:
            raise NotFoundError('Order #{} does not exist!'.format(order_number.value))
        elif order.customer_id.value != user.id:
            raise ForbiddenError('Order #{} is not your!'.format(order_number.value))

        requests = cancel_requests_storage.get_all_by_order_number(order.number)
        return __get_cancellation_requests_response(requests)

    @blueprint.route('/customer/cancellations/get/{request_number}', methods=['GET'], cors=True)
    def cancellation_get(request_number):
        user = __get_user()
        orders_storage = OrderStorageImplementation()
        cancel_requests_storage = CancelRequestStorageImplementation()

        try:
            request_number = CancelRequest.Number(request_number)
        except BaseException:
            raise BadRequestError('Incorrect Input Data!')

        request = cancel_requests_storage.get_by_number(request_number)
        if not request:
            raise NotFoundError('Cancellation Request #{} does not exist!'.format(request_number.value))

        order = orders_storage.load(request.order_number)
        if order.customer_id.value != user.id:
            raise ForbiddenError('Cancellation Request #{} is not your!'.format(request.number.value))

        return __get_cancellation_requests_response(tuple([request]))[0]

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CREATE
    # ------------------------------------------------------------------------------------------------------------------

    def __get_refund_methods_initial_data(order: Order) -> Optional[dict]:
        if not order.was_paid:
            return None

        methods_map = {
            # @todo : payment methods descriptors
            'regular_eft': [{
                'key': refund_method.descriptor,
                'label': refund_method.label,
            } for refund_method in [
                StoreCreditRefundMethod(),
                EftRefundMethod('test', 'test', 'test')
            ]],
            'customer_credit': [{
                'key': refund_method.descriptor,
                'label': refund_method.label,
            } for refund_method in [
                StoreCreditRefundMethod(),
                EftRefundMethod('test', 'test', 'test')
            ]],
            'mobicred': [{
                'key': refund_method.descriptor,
                'label': refund_method.label,
            } for refund_method in [
                StoreCreditRefundMethod(),
                EftRefundMethod('test', 'test', 'test'),
                MobicredRefundMethod(),
            ]],
            'credit_card': [{
                'key': refund_method.descriptor,
                'label': refund_method.label,
            } for refund_method in [
                StoreCreditRefundMethod(),
                EftRefundMethod('test', 'test', 'test'),
                CreditCardRefundMethod()
            ]]
        }

        result = methods_map.get(order.payment_method.descriptor)
        if not result:
            raise Exception('{} does not know, how to work with "{}" payment method!'.format(
                __get_refund_methods_initial_data.__qualname__,
                order.payment_method.descriptor
            ))

        return result

    @blueprint.route('/customer/cancellations/create/get_initial_data/{order_number}', methods=['GET'], cors=True)
    def cancellation_create_get_initial_data(order_number):
        user = __get_user()
        orders_storage = OrderStorageImplementation()
        products_storage = ProductStorageImplementation()

        try:
            order_number = Order.Number(order_number)
        except BaseException:
            raise BadRequestError('Incorrect Input Data!')

        order = orders_storage.load(order_number)
        if not order:
            raise NotFoundError('Order #{} does not exist!'.format(order_number.value))
        elif order.customer_id.value != user.id:
            raise ForbiddenError('Order #{} is not your!'.format(order_number.value))
        elif not order.is_cancellable:
            raise UnprocessableEntityError('Order #{} is not Cancellable!'.format(order.number.value))

        products_map: Dict[str, ProductInterface] = {}
        for item in order.items:
            product = products_map.get(item.simple_sku.value) or products_storage.load(item.simple_sku)
            products_map[item.simple_sku.value] = product

        return {
            'items': [{
                'simple_sku': item.simple_sku.value,
                'product_name': products_map[item.simple_sku.value].name.value,
                'img_url': (
                    products_map[item.simple_sku.value].image_urls[0]
                    if products_map[item.simple_sku.value].image_urls else None
                ),
                'costs': [{
                    'qty': qty,
                    'cost': item.get_refund_cost(Qty(qty)).value
                } for qty in range(1, item.qty_processable.value + 1)],
                'qty_can_cancel': item.qty_processable.value,
            } for item in order.items],
            'refund_methods': __get_refund_methods_initial_data(order),
        }

    @blueprint.route('/customer/cancellations/create/submit', methods=['POST'], cors=True)
    def cancellation_create_submit():
        user = __get_user()
        orders_storage = OrderStorageImplementation()

        try:
            order_number = Order.Number(blueprint.current_request.json_body.get('order_number'))
            items = [{
                'simple_sku': SimpleSku(item.get('simple_sku')),
                'qty': Qty(item.get('qty'))
            } for item in blueprint.current_request.json_body.get('items') or []]
            additional_comment = str(blueprint.current_request.json_body.get('additional_comment') or '') or None

            refund_method_input_data = blueprint.current_request.json_body.get('refund_method') or {}
            refund_method_input_data['type'] = str(refund_method_input_data.get('type') or '')
            refund_method_input_data['params'] = refund_method_input_data.get('params') or {}

        except ValueError:
            raise BadRequestError('Incorrect Input Data!')

        order = orders_storage.load(order_number)
        if not order:
            raise NotFoundError('Order #{} does not exist!'.format(order_number.value))
        elif order.customer_id.value != user.id:
            raise ForbiddenError('Order #{} is not your!'.format(order_number.value))
        elif not order.is_cancellable:
            raise UnprocessableEntityError('Order #{} is not Cancellable!'.format(order.number.value))

        # @todo : refactoring ???
        try:
            if order.was_paid:
                refund_method_instance = None
                for _refund_method_instance in [
                    StoreCreditRefundMethod(),
                    EftRefundMethod('test', 'test', 'test'),
                    MobicredRefundMethod(),
                    CreditCardRefundMethod()
                ]:
                    if _refund_method_instance.descriptor == refund_method_input_data['type']:
                        refund_method_instance = _refund_method_instance.__class__(**refund_method_input_data['params'])
                        break

                if not refund_method_instance:
                    error_message = 'Incorrect Input Data! Refund method {} is not allowed for selected orders!'
                    raise BadRequestError(error_message.format(refund_method_input_data['type']))

                cancellation_request = __cancellation_create_submit_after_payment(
                    order,
                    items,
                    refund_method_instance,
                    additional_comment
                )

                return {
                    'request_number': cancellation_request.number.value
                }
            else:
                __cancellation_create_submit_before_payment(order, items)

                return {
                    'Code': 'Success',
                    'Message': 'Success',
                }
        except ApplicationLogicException as e:
            raise UnprocessableEntityError(str(e))

    def __cancellation_create_submit_before_payment(order: Order, items):
        orders_storage = OrderStorageImplementation()
        products_storage = ProductStorageImplementation()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        def __log_flow(text: str):
            logger.log_simple('Cancellation for Order #{} Before Payment: {}'.format(
                order.number.value,
                text
            ))

        __log_flow('Start - {}'.format([{
            'simple_sku': item['simple_sku'].value,
            'qty': item['qty'].value,
        } for item in items]))

        # updates
        products_to_update = {}
        for item in items:
            products_to_update[item['simple_sku'].value] = products_storage.load(item['simple_sku'])
            products_to_update[item['simple_sku'].value].restore_qty(item['qty'])
            order.cancel_before_payment(item['simple_sku'], item['qty'])

        order_change_event = OrderChangeSqsSenderEvent(order)

        # save order
        __log_flow('Order Saving...')
        orders_storage.save(order)
        __log_flow('Order Saved!')

        # saving products
        __log_flow('Products Saving... {}'.format(tuple(products_to_update.keys())))
        for product in tuple(products_to_update.values()):
            __log_flow('Product {} Saving...'.format(product.simple_sku.value))
            products_storage.update(product)
            __log_flow('Product {} Saved!'.format(product.simple_sku.value))
        __log_flow('Products Saved!')

        # send order
        __log_flow('Order SQS Sending...')
        sqs_sender.send(order_change_event)
        __log_flow('Order SQS Sent!')

        __log_flow('End')

    def __cancellation_create_submit_after_payment(
        order: Order,
        items,
        refund_method: RefundMethodAbstract,
        additional_comment: Optional[str]
    ) -> CancelRequest:
        orders_storage = OrderStorageImplementation()
        cancellation_storage = CancelRequestStorageImplementation()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        def __log_flow(text: str):
            logger.log_simple('Cancellation for Order #{} After Payment: {}'.format(
                order.number.value,
                text
            ))

        __log_flow('Start - {}'.format({
            'items': [{
                'simple_sku': item['simple_sku'].value,
                'qty': item['qty'].value,
            } for item in items],
            'additional_comment': additional_comment
        }))

        # updates
        cancellation_request = CancelRequest(
            CancelRequest.Number(datetime.datetime.now().strftime('%y%j03%f')),
            order.number,
            tuple([CancelRequest.Item(item['simple_sku'], item['qty']) for item in items]),
            refund_method,
            CancelRequest.AdditionalComment(additional_comment) if additional_comment else None
        )

        for item in items:
            order.request_cancellation_after_payment(item['simple_sku'], item['qty'])

        order_change_event = OrderChangeSqsSenderEvent(order)
        cancel_request_event = CancelRequestPaidOrderSqsSenderEvent(cancellation_request)

        # save request
        __log_flow('Cancel Request Saving...')
        cancellation_storage.save(cancellation_request)
        __log_flow('Cancel Request Saved!')

        # save order
        __log_flow('Order Saving...')
        orders_storage.save(order)
        __log_flow('Order Saved!')

        # send request
        __log_flow('Cancel Request SQS Sending...')
        sqs_sender.send(cancel_request_event)
        __log_flow('Cancel Request SQS Sent!')

        # send order
        __log_flow('Order SQS Sending...')
        sqs_sender.send(order_change_event)
        __log_flow('Order SQS Sent!')

        __log_flow('End')

        return cancellation_request


# ----------------------------------------------------------------------------------------------------------------------

