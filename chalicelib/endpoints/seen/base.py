from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.seen.storage import SeenStorageImplementation
from chalicelib.libs.seen.service import SeenAppService


seen_blueprint = Blueprint(__name__)


def __get_user_id() -> str:
    return seen_blueprint.current_request.current_user.user_id


def __response_seen(user_id) -> dict:
    seen_storage = SeenStorageImplementation()
    seen = seen_storage.load(user_id)
    return seen.items if seen else []

# ------------------------------------------------------------------------------------------------------------------
#                                               VIEW SEEN
# ------------------------------------------------------------------------------------------------------------------

@seen_blueprint.route('/list', methods=['GET'], cors=True)
def seen_list():
    try:
        user_id = __get_user_id()
        if not user_id:
            raise HttpAuthenticationRequiredException()
        return __response_seen(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)

# ------------------------------------------------------------------------------------------------------------------
#                                               ADD PRODUCT
# ------------------------------------------------------------------------------------------------------------------

@seen_blueprint.route('/add-product', methods=['POST'], cors=True)
def seen_add_product():
    seen_app_service = SeenAppService()

    try:
        request_data = seen_blueprint.current_request.json_body
        sku = str(request_data.get('sku', '')).strip()
        sku = sku if len(sku) > 0 else None
        if not sku:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        if not user_id:
            raise HttpAuthenticationRequiredException()

        seen_app_service.add_seen_product(user_id, sku)
        return __response_seen(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)

# ------------------------------------------------------------------------------------------------------------------
#                                               ADD PRODUCTS
# ------------------------------------------------------------------------------------------------------------------

@seen_blueprint.route('/add-products', methods=['POST'], cors=True)
def seen_add_product():
    seen_app_service = SeenAppService()

    try:
        request_data = seen_blueprint.current_request.json_body
        skus = request_data.get('skus', '')
        skus = skus if len(skus) > 0 else None
        if not skus:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        if not user_id:
            raise HttpAuthenticationRequiredException()
        seen_app_service.add_seen_products(user_id, skus)
        return __response_seen(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)

# ------------------------------------------------------------------------------------------------------------------
#                                               REMOVE PRODUCT
# ------------------------------------------------------------------------------------------------------------------

@seen_blueprint.route('/remove-product', methods=['DELETE'], cors=True)
def seen_remove_product():
    seen_app_service = SeenAppService()

    try:
        request_data = seen_blueprint.current_request.json_body
        sku = str(request_data.get('sku', '')).strip()
        sku = sku if len(sku) > 0 else None
        if not sku:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        if not user_id:
            raise HttpAuthenticationRequiredException()
        seen_app_service.remove_seen_product(user_id, sku)
        return __response_seen(user_id)
    except BaseException as e:
        return http_response_exception_or_throw(e)

# ------------------------------------------------------------------------------------------------------------------
#                                               PRODUCT IS ADDED
# ------------------------------------------------------------------------------------------------------------------

@seen_blueprint.route('/product-is-added', methods=['POST'], cors=True)
def seen_remove_product():
    seen_app_service = SeenAppService()

    try:
        request_data = seen_blueprint.current_request.json_body
        sku = str(request_data.get('sku', '')).strip()
        sku = sku if len(sku) > 0 else None
        if not sku:
            raise HttpIncorrectInputDataException()

        user_id = __get_user_id()
        if not user_id:
            raise HttpAuthenticationRequiredException()
        return seen_app_service.product_is_in_seen(user_id, sku)
    except BaseException as e:
        return http_response_exception_or_throw(e)

# ------------------------------------------------------------------------------------------------------------------

