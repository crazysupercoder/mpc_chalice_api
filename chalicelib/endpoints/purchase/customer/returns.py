import os
import uuid
import hashlib
import datetime
from chalice import \
    Blueprint,\
    UnauthorizedError,\
    NotFoundError,\
    ForbiddenError,\
    BadRequestError,\
    UnprocessableEntityError
from chalicelib.extensions import *
from chalicelib.libs.core.file_storage import FileStorageImplementation
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.purchase.core import Id, OrderNumber, SimpleSku, Cost, Qty, ReturnRequest
from chalicelib.libs.purchase.returns.storage import ReturnRequestStorageImplementation
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.purchase.payment_methods.refund_methods import \
    StoreCreditRefundMethod, \
    EftRefundMethod, \
    CreditCardRefundMethod, \
    MobicredRefundMethod
from chalicelib.libs.purchase.returns.delivery_methods import \
    HandDeliveryMethod, \
    CourierOrPostOffice, \
    RunwaysaleToCollect
from chalicelib.libs.purchase.returns.sqs import ReturnRequestChangeSqsSenderEvent


# @TODO : REFACTORING !!!


def register_customer_returns(blueprint: Blueprint):
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise UnauthorizedError('Authentication is required!')

        return user

    # ------------------------------------------------------------------------------------------------------------------
    #                                                       LIST
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/returns/list', methods=['GET'], cors=True)
    def returns_list():
        returns_storage = ReturnRequestStorageImplementation()
        orders_storage = OrderStorageImplementation()

        customer_id = Id(__get_user().id)
        returns = returns_storage.get_all_for_customer(customer_id)

        orders_map = {}
        _order_numbers = [
            return_request_item.order_number
            for return_request in returns
            for return_request_item in return_request.items
        ]
        _orders = orders_storage.get_all_by_numbers(tuple(_order_numbers))
        for _order_number in _order_numbers:
            for _order in _orders:
                if _order.number.value == _order_number.value:
                    orders_map[_order_number.value] = _order
                    break
            else:
                raise ValueError('{} - Unable to find Order #{} for Customer\'s #{} returns'.format(
                    returns_list.__qualname__,
                    _order_number.value,
                    __get_user().id
                ))

        response = []
        for return_request in returns:
            items = []
            for return_item in return_request.items:
                order = orders_map[return_item.order_number.value]
                items.append({
                    'order_number': return_item.order_number.value,
                    'ordered_at': order.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'cost': return_item.cost.value,
                })

            response.append({
                'request_number': return_request.number.value,
                'requested_at': return_request.requested_at.strftime('%Y-%m-%d %H:%M:%S'),
                'items': items,
                'status': {
                    'value': return_request.total_status.value,
                    'label': return_request.total_status.label,
                }
            })

        return response

    # ------------------------------------------------------------------------------------------------------------------
    #                                                       VIEW
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/returns/view/{return_number}', methods=['GET'], cors=True)
    def returns_view(return_number):
        customer_id = Id(__get_user().id)
        returns_storage = ReturnRequestStorageImplementation()
        orders_storage = OrderStorageImplementation()
        products_storage = ProductStorageImplementation()

        return_request = returns_storage.load(ReturnRequest.Number(return_number))
        if not return_request:
            raise NotFoundError('Return Request #{} does not exist!'.format(return_number))
        elif return_request.customer_id != customer_id:
            raise ForbiddenError('It is not your Return Request!')

        response = {
            'request_number': return_request.number.value,
            'requested_at': return_request.requested_at.strftime('%Y-%m-%d %H:%M:%S'),
            'items': [],
            'delivery_method': return_request.delivery_method.label,
            'refund_method': return_request.refund_method.label,
            'status': {
                'value': return_request.total_status.value,
                'label': return_request.total_status.label,
            }
        }

        orders_map = {}
        products_map = {}
        for return_item in return_request.items:
            product = products_map.get(return_item.simple_sku.value) or products_storage.load(return_item.simple_sku)
            products_map[return_item.simple_sku.value] = product

            order = orders_map.get(return_item.order_number.value) or orders_storage.load(return_item.order_number)
            orders_map[return_item.order_number.value] = order

            response['items'].append({
                'order_number': return_item.order_number.value,
                'simple_sku': return_item.simple_sku.value,
                'product_name': product.name.value,
                'product_image_url': product.image_urls[0] if product.image_urls else None,
                'size_name': product.size_name.value,
                'ordered_at': order.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'cost': return_item.cost.value,
                'qty': return_item.qty.value,
                'status': return_item.status.label,
                'reason': return_item.reason.label,
                'attached_files': [{'url': file.url} for file in return_item.attached_files],
                'additional_comment': return_item.additional_comment.value if return_item.additional_comment else None,
            })

        return response

    # ------------------------------------------------------------------------------------------------------------------
    #                                                       CREATE
    # ------------------------------------------------------------------------------------------------------------------

    def __get_initial_data():
        orders_storage = OrderStorageImplementation()
        products_storage = ProductStorageImplementation()

        orders = []
        products_map = {}

        # "...credit-card should be allowed only when one of selected orders was paid by credit card,
        # but eft and credits should be available for all return-requests..."
        payment_refund_methods_map = {
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

        for order in orders_storage.get_all_for_customer(Id(__get_user().id)):
            if not order.is_returnable:
                continue

            items = []
            for item in order.items:
                product = products_map.get(item.simple_sku.value) or products_storage.load(item.simple_sku)
                products_map[item.simple_sku.value] = product

                items.append({
                    'simple_sku': item.simple_sku.value,
                    'product_name': product.name.value,
                    'img_url': product.image_urls[0] if product.image_urls else None,
                    'costs': [{
                        'qty': qty,
                        'cost': item.get_refund_cost(Qty(qty)).value
                    } for qty in range(1, item.qty_processable.value + 1)],
                    'qty_can_return': item.qty_processable.value,
                })

            orders.append({
                'order_number': order.number.value,
                'ordered_at': order.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'can_be_returned_till': order.is_returnable_till.strftime('%Y-%m-%d %H:%M:%S'),
                'items': items,
                'refund_methods': payment_refund_methods_map[order.payment_method.descriptor]
            })

        return {
            'reasons': [{
                'key': reason.descriptor,
                'label': reason.label,
            } for reason in [
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.TOO_BIG),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.TOO_SMALL),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.SELECTED_WRONG_SIZE),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.DONT_LIKE_IT),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.NOT_HAPPY_WITH_QTY),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.RECEIVED_WRONG_SIZE),
                ReturnRequest.Item.Reason(ReturnRequest.Item.Reason.RECEIVED_DAMAGED),
            ]],
            'delivery_methods': [{
                'key': delivery_method.descriptor,
                'label': delivery_method.label,
            } for delivery_method in [
                HandDeliveryMethod(),
                CourierOrPostOffice(),
                RunwaysaleToCollect()
            ]],
            'orders': orders,
        }

    @blueprint.route('/customer/returns/create/get_initial_data', methods=['GET'], cors=True)
    def returns_create_list_orders():
        return __get_initial_data()

    @blueprint.route(
        '/customer/returns/create/upload_file',
        methods=['POST'], cors=True, content_types=['application/octet-stream']
    )
    def returns_create_upload_file():
        file_storage = FileStorageImplementation()

        user_id = __get_user().id

        # @todo : create file uploader or so... ???

        max_size_mb = 4
        size_in_bytes = len(blueprint.current_request.raw_body)
        if size_in_bytes > max_size_mb * 1024 * 1024:
            raise BadRequestError('Uploaded file max size is {} Mb!'.format(max_size_mb))
        elif not size_in_bytes:
            raise BadRequestError('Uploaded file cannot be empty!')

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
            raise BadRequestError('Mime-type is not supported!')

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

    @blueprint.route('/customer/returns/create/submit', methods=['POST'], cors=True)
    def returns_create_submit():
        """
        POST : {
            items: [
                {
                    order_number: str,
                    simple_sku: str,
                    qty: int,
                    reason: str,
                    file_ids: [str, ...],
                    additional_comment: str|null,
                },
                ...
            ],
            delivery_method: str,
            refund_method: {
                type: str,
                params: {
                    # credit_card_eft
                    account_holder_name: str,
                    account_number: str,
                    branch_code: str
                }
            }
        }
        """

        user_id = __get_user().id
        now = datetime.datetime.now()
        returns_storage = ReturnRequestStorageImplementation()
        orders_storage = OrderStorageImplementation()
        file_storage = FileStorageImplementation()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        # 1. Check input
        # -------------------------------

        request_data = blueprint.current_request.json_body or {}
        input_items = request_data.get('items')
        if not input_items or not isinstance(input_items, (list, tuple, set)):
            raise BadRequestError('Incorrect Input Data! Parameter "items" is required!')
        elif sum([
            not isinstance(item, dict)
            or not isinstance(item.get('order_number'), str)
            or not isinstance(item.get('simple_sku'), str)
            or not isinstance(item.get('qty'), int)
            or not isinstance(item.get('reason'), str)
            or not isinstance(item.get('file_ids'), (list, tuple, set))
            or sum([not isinstance(file_id, str) for file_id in item['file_ids']]) > 0
            or not (item.get('additional_comment') is None or isinstance(item.get('additional_comment'), str))
            for item in input_items
        ]) > 0:
            raise BadRequestError('Incorrect Input Data! Incorrect "items" structure!')

        delivery_method_input_descriptor = request_data.get('delivery_method')
        if not delivery_method_input_descriptor or not isinstance(delivery_method_input_descriptor, str):
            raise BadRequestError('Incorrect Input Data! Parameter "delivery_method" is required!')

        refund_method_input_data = request_data.get('refund_method')
        if not refund_method_input_data:
            raise BadRequestError('Incorrect Input Data! Parameter "refund_method" is required!')
        elif (
            not isinstance(refund_method_input_data, dict)
            or not isinstance(refund_method_input_data.get('type'), str)
            or not isinstance(refund_method_input_data.get('params'), dict)
        ):
            raise BadRequestError('Incorrect Input Data! Parameter "refund_method" is incorrect!')

        # collect control data
        initial_data = __get_initial_data()
        control_data = {
            'reasons': [reason['key'] for reason in initial_data['reasons']],
            'delivery_methods': [_delivery_method['key'] for _delivery_method in initial_data['delivery_methods']],
            'orders': {},
        }
        for order_data in initial_data['orders']:
            order_number = order_data['order_number']
            for order_data_item in order_data['items']:
                simple_sku = order_data_item['simple_sku']
                qty = order_data_item['qty_can_return']
                control_data['orders'][order_number] = control_data['orders'].get(order_number) or {}
                control_data['orders'][order_number][simple_sku] = qty

        # validate input data
        if (
            # items
            sum([
                item['order_number'] not in control_data['orders'].keys()
                or item['simple_sku'] not in control_data['orders'][item['order_number']].keys()
                or item['qty'] not in range(1, control_data['orders'][item['order_number']][item['simple_sku']] + 1)
                or item['reason'] not in control_data['reasons']
                or sum([not file_id.strip() or not file_storage.get(file_id) for file_id in item['file_ids']]) > 0
                or (item['additional_comment'] is not None and len(item['additional_comment']) > 255)
                for item in input_items
            ]) > 0

            # delivery method
            or delivery_method_input_descriptor not in control_data['delivery_methods']

            # refund method (method structure/data check)
            or refund_method_input_data['type'] not in [
                EftRefundMethod('test', 'test', 'test').descriptor,
                StoreCreditRefundMethod().descriptor,
                MobicredRefundMethod().descriptor,
                CreditCardRefundMethod().descriptor,
            ]
            or (
                refund_method_input_data['type'] == EftRefundMethod('test', 'test', 'test').descriptor
                and (
                    not isinstance(refund_method_input_data.get('params', {}).get('account_holder_name'), str)
                    or not refund_method_input_data['params'].get('account_holder_name')
                    or not isinstance(refund_method_input_data.get('params', {}).get('account_number'), str)
                    or not refund_method_input_data['params'].get('account_number')
                    or not isinstance(refund_method_input_data.get('params', {}).get('branch_code'), str)
                    or not refund_method_input_data['params'].get('branch_code')
                )
            )
            or (
                refund_method_input_data['type'] in (
                    StoreCreditRefundMethod().descriptor,
                    MobicredRefundMethod().descriptor,
                    CreditCardRefundMethod().descriptor,
                )
                and len(refund_method_input_data['params']) > 0
            )
        ):
            raise BadRequestError('Incorrect Input Data! Incorrect values!')

        # check duplicates in order
        if len(set([str(item['order_number']) + str(item['simple_sku']) for item in input_items])) != len(input_items):
            raise BadRequestError('Incorrect Input Data! Input items has duplicates!')

        # check refund methods
        # "...credit-card should be allowed only when one of selected orders was paid by credit card,
        # but eft and credits should be available for all return-requests..."
        _allowed_refund_methods_keys = []
        for item in input_items:
            _order_refund_method_keys = [
                _order_refund_method['key']
                for _order in initial_data['orders'] if _order['order_number'] == item['order_number']
                for _order_refund_method in _order['refund_methods']
            ]

            # intersection of all input orders
            if len(_allowed_refund_methods_keys) == 0:
                _allowed_refund_methods_keys = _order_refund_method_keys
            else:
                _allowed_refund_methods_keys = [
                    key for key in _allowed_refund_methods_keys
                    if key in _order_refund_method_keys
                ]
        if refund_method_input_data['type'] not in _allowed_refund_methods_keys:
            raise BadRequestError('Incorrect Input Data! Refund method {} is not allowed for selected orders!'.format(
                refund_method_input_data['type']
            ))

        # 2. Create Return Request entity
        # -------------------------------

        return_request_items = []
        for item in input_items:
            order_number = item['order_number']
            simple_sku = item['simple_sku']
            qty = item['qty']

            cost = None
            for initial_order in initial_data['orders']:
                if initial_order['order_number'] == order_number:
                    for initial_item in initial_order['items']:
                        if initial_item['simple_sku'] == simple_sku:
                            cost = tuple(filter(lambda x: x.get('qty') == qty, initial_item['costs']))[0].get('cost')
                            break

            reason = ReturnRequest.Item.Reason(item['reason'])

            attached_files = tuple([
                ReturnRequest.Item.AttachedFile(file_storage.get(file_id).url)
                for file_id in item['file_ids']
            ])

            additional_comment = item.get('additional_comment') if item.get('additional_comment') else None
            additional_comment = ReturnRequest.Item.AdditionalComment(additional_comment) if additional_comment else None

            return_request_items.append(ReturnRequest.Item(
                OrderNumber(order_number),
                SimpleSku(simple_sku),
                Qty(qty),
                Cost(cost),
                reason,
                attached_files,
                additional_comment
            ))

        delivery_method_instance = None
        for _delivery_method_instance in [
            HandDeliveryMethod(),
            CourierOrPostOffice(),
            RunwaysaleToCollect()
        ]:
            if _delivery_method_instance.descriptor == delivery_method_input_descriptor:
                delivery_method_instance = _delivery_method_instance
                break

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

        return_request = ReturnRequest(
            Id(user_id),
            ReturnRequest.Number(now.strftime('%y%j03%f')),
            tuple(return_request_items),
            delivery_method_instance,
            refund_method_instance
        )

        # 3. Modify orders qty
        # -------------------------------

        modified_orders = {}
        for return_item in return_request.items:
            order = modified_orders.get(return_item.order_number.value) or orders_storage.load(return_item.order_number)
            order.request_return(return_item.simple_sku, return_item.qty)
            modified_orders[order.number.value] = order

        # 4. Save changes
        # -------------------------------

        def __log_flow(text: str) -> None:
            logger.log_simple('Return Request #{} - Creation : {}'.format(return_request.number.value, text))

        __log_flow('Start')

        __log_flow('Saving Return Request...')
        returns_storage.save(return_request)
        __log_flow('Saving Return Request - Done!')

        __log_flow('Saving Orders...')
        for order in tuple(modified_orders.values()):
            __log_flow('Saving Order #{}...'.format(order.number.value))
            orders_storage.save(order)
            __log_flow('Saving Order #{} - Done!'.format(order.number.value))
        __log_flow('Saving Orders - Done!')

        # 5. Send SQS
        # -------------------------------

        __log_flow('SQS Sending Return Request...')
        sqs_sender.send(ReturnRequestChangeSqsSenderEvent(return_request))
        __log_flow('SQS Sending Return Request - Done!')

        __log_flow('End')

        return {
            'request_number': return_request.number.value
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                       REJECT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/returns/reject', methods=['DELETE'], cors=True)
    def returns_reject():
        returns_storage = ReturnRequestStorageImplementation()
        orders_storage = OrderStorageImplementation()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        request_data = blueprint.current_request.json_body
        request_number = str(request_data.get('request_number') or '').strip() or None
        if not request_number:
            raise BadRequestError('"request_number" is required')

        order_number_value = str(request_data.get('order_number') or '').strip() or None
        if not order_number_value:
            raise BadRequestError('"order_number" is required')

        simple_sku_value = str(request_data.get('simple_sku') or '').strip() or None
        if not simple_sku_value:
            raise BadRequestError('"simple_sku" is required')

        return_request = returns_storage.load(ReturnRequest.Number(request_number))
        if not return_request:
            raise NotFoundError('Return Request #{} does not exist!'.format(request_number))

        if return_request.customer_id.value != __get_user().id:
            raise ForbiddenError('It is not your Return Request!')

        order_number = OrderNumber(order_number_value)
        simple_sku = SimpleSku(simple_sku_value)
        order = orders_storage.load(order_number)
        if (
            not order
            or not len([item for item in return_request.items if item.order_number == order_number])
            or not len([item for item in order.items if item.simple_sku == simple_sku])
        ):
            raise NotFoundError('Product "{}" is not requested in Return Request #{} for Order #{}!'.format(
                simple_sku.value,
                return_request.number.value,
                order_number.value,
            ))

        # update values
        try:
            qty = return_request.get_item_qty(order_number, simple_sku)
            return_request.make_item_rejected(order_number, simple_sku)
            order.reject_return(simple_sku, qty)
        except ApplicationLogicException as e:
            raise UnprocessableEntityError(str(e))

        # save updates

        def __log_flow(text: str) -> None:
            logger.log_simple('Return Request #{} - Rejecting : {}'.format(return_request.number.value, text))

        __log_flow('Start')

        # saving
        __log_flow('Saving - Start')
        try:
            __log_flow('Order #{} Saving...'.format(order_number.value))
            orders_storage.save(order)
            __log_flow('Order #{} Saving - Done!'.format(order_number.value))

            __log_flow('Return Request Saving...')
            returns_storage.save(return_request)
            __log_flow('Return Request Saving - Done!')
        except ApplicationLogicException as e:
            __log_flow('Not Saved because of Error : {}'.format(str(e)))
            raise UnprocessableEntityError(str(e))
        __log_flow('Saving - End')

        # send sqs
        __log_flow('SQS Sending - Start')
        try:
            __log_flow('Return Request SQS Sending...')
            sqs_sender.send(ReturnRequestChangeSqsSenderEvent(return_request))
            __log_flow('Return Request SQS Sending - Done!')
        except BaseException as e:
            __log_flow('Return Request SQS Sending - Not done because of Error : {}'.format(str(e)))
            logger.log_exception(e)
        __log_flow('SQS Sending - End')

        __log_flow('End')

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   SEND
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/returns/send', methods=['PUT'], cors=True)
    def returns_send_item():
        returns_storage = ReturnRequestStorageImplementation()
        sqs_sender = SqsSenderImplementation()
        logger = Logger()

        request_data = blueprint.current_request.json_body
        request_number = str(request_data.get('request_number') or '').strip() or None
        if not request_number:
            raise BadRequestError('"request_number" is required')

        return_request = returns_storage.load(ReturnRequest.Number(request_number))
        if not return_request:
            raise NotFoundError('Return Request #{} does not exist!'.format(request_number))

        if return_request.customer_id.value != __get_user().id:
            raise ForbiddenError('It is not your Return Request!')

        try:
            return_request.make_package_sent()
        except ApplicationLogicException as e:
            raise UnprocessableEntityError(str(e))

        def __log_flow(text: str) -> None:
            logger.log_simple('Return Request #{} - Sending : {}'.format(return_request.number.value, text))

        __log_flow('Start')

        # change status
        __log_flow('Saving - Start')
        try:
            __log_flow('Return Request Saving...')
            returns_storage.save(return_request)
            __log_flow('Return Request Saving - Done!')
        except BaseException as e:
            __log_flow('Return Request Saving - Not done because of Error: {}!'.format(str(e)))
            raise e
        __log_flow('Saving - End')

        # send sqs
        __log_flow('SQS Sending - Start')
        try:
            __log_flow('Return Request SQS Sending...')
            sqs_sender.send(ReturnRequestChangeSqsSenderEvent(return_request))
            __log_flow('Return Request SQS Sending - Done!')
        except BaseException as e:
            logger.log_exception(e)
            __log_flow('Return Request SQS Sending - Not done because of Error: {}!'.format(str(e)))
        __log_flow('SQS Sending - End')

        __log_flow('End')

    # ------------------------------------------------------------------------------------------------------------------

