import math
from typing import Tuple
from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Id, Cart
from chalicelib.libs.purchase.cart.storage import CartStorageImplementation
from chalicelib.libs.purchase.cart.service import CartAppService
from chalicelib.libs.purchase.order.dtd_calculator import DtdCalculatorImplementation
from chalicelib.libs.models.mpc.Product import Product as MpcProduct


def register_cart(blueprint: Blueprint) -> None:
    def __get_cart_id() -> str:
        if blueprint.current_request.current_user.is_anyonimous:
            return blueprint.current_request.current_user.session_id

        return blueprint.current_request.current_user.id

    def __response_cart(cart_id) -> dict:
        cart_storage = CartStorageImplementation()
        dtd_calculator = DtdCalculatorImplementation()

        def __return(
            cart_items: Tuple[Cart.Item],
            original_subtotal: float,
            current_subtotal: float,
            current_subtotal_vat_amount: float
        ):
            tier = blueprint.current_request.current_user.profile.tier

            # fbucks available to spend
            available_fbucks_amount = None
            if not tier['is_neutral'] and not blueprint.current_request.current_user.is_anyonimous:
                """"""
                # @TODO : REFACTORING !!!
                from chalicelib.libs.purchase.customer.sqs import FbucksChargeSqsHandler
                see = FbucksChargeSqsHandler
                """"""
                from chalicelib.settings import settings
                from chalicelib.libs.core.elastic import Elastic
                __fbucks_customer_amount_elastic = Elastic(
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                    settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                )
                fbucks_amount_row = __fbucks_customer_amount_elastic.get_data(blueprint.current_request.current_user.id)
                available_fbucks_amount = fbucks_amount_row['amount'] or 0 if fbucks_amount_row else 0

            products = MpcProduct()
            items_data = []
            for cart_item in cart_items:
                product = products.getRawDataBySimpleSku(cart_item.simple_sku.value)
                product_sizes = product.get('sizes', []) if product else ()
                size = tuple(filter(lambda s: s.get('simple_sku') == cart_item.simple_sku.value, product_sizes))[0]
                dtd = dtd_calculator.calculate(cart_item.simple_sku, cart_item.qty)

                item_fbucks = None
                if not tier['is_neutral'] and not blueprint.current_request.current_user.is_anyonimous:
                    item_fbucks = math.ceil(cart_item.current_cost.value * tier['discount_rate'] / 100)

                items_data.append({
                    'sku': product['sku'],
                    'simple_sku': cart_item.simple_sku.value,
                    'name': product.get('title'),
                    'brand_name': product.get('brand'),
                    'size_name': size.get('size'),
                    'image_url': product.get('image', {}).get('src', None),
                    'qty_available': int(size.get('qty')),
                    'qty_added': cart_item.qty.value,
                    'is_added_over_limit': cart_item.is_added_over_limit,
                    'product_original_price': cart_item.product_original_price.value,
                    'product_current_price': cart_item.product_current_price.value,
                    'original_cost': cart_item.original_cost.value,
                    'current_cost': cart_item.current_cost.value,
                    'dtd': {
                        'occasion': {
                            'name': dtd.occasion.name.value,
                            'description': dtd.occasion.description.value,
                        } if dtd.occasion else None,
                        'date_from': dtd.date_from.strftime('%Y-%m-%d'),
                        'date_to': dtd.date_to.strftime('%Y-%m-%d'),
                        'working_days_from': dtd.working_days_from,
                        'working_days_to': dtd.working_days_to,
                    },
                    'fbucks': item_fbucks,
                })

            return {
                'items': items_data,
                'original_subtotal': original_subtotal,
                'current_subtotal': current_subtotal,
                'current_subtotal_vat_amount': current_subtotal_vat_amount,
                'available_fbucks_amount': available_fbucks_amount,
            }

        cart_id = Id(cart_id)
        cart = cart_storage.get_by_id(cart_id)
        return __return(
            cart.items if cart else tuple(),
            cart.original_subtotal.value if cart else 0.0,
            cart.current_subtotal.value if cart else 0.0,
            cart.current_subtotal_vat_amount.value if cart else 0.0
        )

    # ------------------------------------------------------------------------------------------------------------------
    #                                               VIEW CART
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/cart/view', methods=['GET'], cors=True)
    def cart_view():
        try:
            cart_id = __get_cart_id()
            return __response_cart(cart_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               ADD PRODUCT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/cart/add-product', methods=['POST'], cors=True)
    def cart_add_product():
        cart_app_service = CartAppService()

        try:
            request_data = blueprint.current_request.json_body
            simple_sku = str(request_data.get('simple_sku', '')).strip()
            simple_sku = simple_sku if len(simple_sku) > 0 else None
            qty = int(str(request_data.get('qty', '0')).strip())
            qty = qty if qty > 0 else None
            if not simple_sku or not qty:
                raise HttpIncorrectInputDataException()

            cart_id = __get_cart_id()
            cart_app_service.add_cart_product(cart_id, simple_sku, qty)
            return __response_cart(cart_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               SET PRODUCT QTY
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/cart/set-product-qty', methods=['PUT'], cors=True)
    def cart_set_product_qty():
        cart_app_service = CartAppService()

        try:
            request_data = blueprint.current_request.json_body
            simple_sku = str(request_data.get('simple_sku', '')).strip()
            simple_sku = simple_sku if len(simple_sku) > 0 else None
            qty = int(str(request_data.get('qty', '0')).strip())
            if not simple_sku or qty < 0:
                raise HttpIncorrectInputDataException()

            cart_id = __get_cart_id()
            if qty == 0:
                cart_app_service.remove_cart_product(cart_id, simple_sku)
            else:
                cart_app_service.set_cart_product_qty(cart_id, simple_sku, qty)

            return __response_cart(cart_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               REMOVE PRODUCT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/cart/remove-product', methods=['DELETE'], cors=True)
    def cart_remove_product():
        cart_app_service = CartAppService()

        try:
            request_data = blueprint.current_request.json_body
            simple_sku = str(request_data.get('simple_sku', '')).strip()
            simple_sku = simple_sku if len(simple_sku) > 0 else None
            if not simple_sku:
                raise HttpIncorrectInputDataException()

            cart_id = __get_cart_id()
            cart_app_service.remove_cart_product(cart_id, simple_sku)
            return __response_cart(cart_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------

