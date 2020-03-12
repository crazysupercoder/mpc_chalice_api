from typing import Union, Tuple, List
from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Id, Order
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.models.mpc.Product import Product as MpcProducts


def register_customer_orders(blueprint: Blueprint) -> None:
    def __get_user_id() -> str:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user.id

    def __orders_response(orders: Union[Tuple[Order], List[Order]]) -> Tuple[dict]:
        products = MpcProducts()

        tier = blueprint.current_request.current_user.profile.tier

        products_map = {}
        for order in orders:
            for item in order.items:
                products_map[item.simple_sku.value] = None

        _existed_products = products.getRawDataBySimpleSkus(tuple(products_map.keys()))
        for _simple_sku in tuple(products_map.keys()):
            for _existed_product in _existed_products:
                if _simple_sku in [size['simple_sku'] for size in _existed_product['sizes']]:
                    products_map[_simple_sku] = _existed_product
                    break
            else:
                raise ValueError('{} - Unable to find Product "{}" for Customer\'s #{} orders'.format(
                    __orders_response.__qualname__,
                    _simple_sku,
                    __get_user_id()
                ))

        response = []
        for order in orders:
            delivery_address = order.delivery_address

            order_items = []
            for order_item in order.items:
                product = products_map[order_item.simple_sku.value]
                product_sizes = product.get('sizes', []) if product else tuple()
                size = tuple(filter(lambda s: s.get('simple_sku') == order_item.simple_sku.value, product_sizes))[0]

                item_fbucks = None
                if not tier['is_neutral'] and not blueprint.current_request.current_user.is_anyonimous:
                    item_fbucks = order_item.fbucks_earnings.value

                order_items.append({
                    'sku': product['sku'],
                    'event_code': order_item.event_code.value,
                    'simple_sku': order_item.simple_sku.value,
                    'name': product.get('title'),
                    'brand_name': product.get('brand'),
                    'size_name': size.get('size'),
                    'image_url': product.get('image', {}).get('src', None),
                    'qty': order_item.qty_ordered.value,
                    'dtd': {
                        'occasion': {
                            'name': order_item.dtd.occasion.name.value,
                            'description': order_item.dtd.occasion.description.value,
                        } if order_item.dtd.occasion else None,
                        'date_from': order_item.dtd.date_from.strftime('%Y-%m-%d'),
                        'date_to': order_item.dtd.date_to.strftime('%Y-%m-%d'),
                        'working_days_from': order_item.dtd.working_days_from,
                        'working_days_to': order_item.dtd.working_days_to,
                    },
                    'product_original_price': order_item.product_original_price.value,
                    'product_current_price': order_item.product_current_price.value,
                    'total_cost': order_item.total_current_cost_ordered.value,
                    'fbucks': item_fbucks,
                })

            response.append({
                'order_number': order.number.value,
                'order_items': order_items,
                'delivery_address': {
                    'recipient_name': delivery_address.recipient_name,
                    'phone_number': delivery_address.phone_number,
                    'street_address': delivery_address.street_address,
                    'suburb': delivery_address.suburb,
                    'city': delivery_address.city,
                    'province': delivery_address.province,
                    'complex_building': delivery_address.complex_building,
                    'postal_code': delivery_address.postal_code,
                    'business_name': delivery_address.business_name,
                    'special_instructions': delivery_address.special_instructions,
                },
                'subtotal': order.subtotal_current_cost_ordered.value,
                'subtotal_vat_amount': order.subtotal_vat_amount.value,
                'delivery_cost': order.delivery_cost.value,
                'credits_spent': order.credit_spent_amount.value,
                'total_ordered': order.total_current_cost_ordered.value,
                'payment_method': {
                    'descriptor': order.payment_method.descriptor,
                    'label': order.payment_method.label,
                } if order.payment_method else None,
                'status': {
                    'value': order.status.value,
                    'label': order.status.label,
                },
                'created_at': order.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'is_cancellable': order.is_cancellable,
            })

        return tuple(response)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               LIST
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/orders/list', methods=['GET'], cors=True)
    def orders_list():
        orders_storage = OrderStorageImplementation()

        try:
            user_id = __get_user_id()
            customer_id = Id(user_id)
            orders = orders_storage.get_all_for_customer(customer_id)
            return __orders_response(orders)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               VIEW
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/orders/view/{order_number}', methods=['GET'], cors=True)
    def orders_view(order_number):
        orders_storage = OrderStorageImplementation()

        try:
            order_number = str(order_number).strip()
            if not order_number:
                raise HttpIncorrectInputDataException('order_number is required!')

            order_number = Order.Number(order_number)
            order = orders_storage.load(order_number)
            if not order:
                raise HttpNotFoundException('Order does not exist!')

            user_id = __get_user_id()
            customer_id = Id(user_id)
            if order.customer_id != customer_id:
                raise HttpAccessDenyException()

            return __orders_response([order])[0]
        except BaseException as e:
            return http_response_exception_or_throw(e)

