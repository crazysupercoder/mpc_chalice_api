from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.wish.storage import WishStorageImplementation
from chalicelib.libs.wish.service import WishAppService
from chalicelib.libs.models.mpc.Product import Product


wish_blueprint = Blueprint(__name__)


def __get_user_id() -> str:
    return wish_blueprint.current_request.current_user.user_id


def __response_wish(user_id) -> dict:
    wish_storage = WishStorageImplementation()

    def __return(wish_items):
        products = Product()
        items_data = []
        for wish_item in wish_items:
            product = products.get_raw_data(wish_item, True)
            items_data.append(product)

        return {
            'items': items_data,
        }

    wish = wish_storage.load(user_id)
    return __return(
        wish.items if wish else [],
    )


# ------------------------------------------------------------------------------------------------------------------
#                                               VIEW WISH
# ------------------------------------------------------------------------------------------------------------------


@wish_blueprint.route('/view', methods=['GET'], cors=True)
def wish_list():
    try:
        user_id = __get_user_id()
        return __response_wish(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)


# ------------------------------------------------------------------------------------------------------------------
#                                               ADD PRODUCT
# ------------------------------------------------------------------------------------------------------------------


@wish_blueprint.route('/add-product', methods=['POST'], cors=True)
def wish_add_product():
    wish_app_service = WishAppService()

    try:
        request_data = wish_blueprint.current_request.json_body
        sku = str(request_data.get('sku', '')).strip()
        sku = sku if len(sku) > 0 else None
        if not sku:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        wish_app_service.add_wish_product(user_id, sku)
        return __response_wish(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)


# ------------------------------------------------------------------------------------------------------------------
#                                               REMOVE PRODUCT
# ------------------------------------------------------------------------------------------------------------------


@wish_blueprint.route('/remove-product', methods=['DELETE'], cors=True)
def wish_remove_product():
    wish_app_service = WishAppService()
    try:
        request_data = wish_blueprint.current_request.json_body
        sku = str(request_data.get('sku', '')).strip()
        sku = sku if len(sku) > 0 else None
        if not sku:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        wish_app_service.remove_wish_product(user_id, sku)
        return __response_wish(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)


# ------------------------------------------------------------------------------------------------------------------

