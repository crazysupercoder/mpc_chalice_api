import math
from typing import Optional, List
from chalice import Blueprint, BadRequestError
from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.models.mpc.Product import Product, ProductSearchCriteria
from chalicelib.libs.models.mpc.product_tracking import ProductsTrackingModel
from chalicelib.libs.models.ml.scored_products import ScoredProduct
from chalicelib.libs.seen.service import SeenAppService
from chalicelib.libs.purchase.core.values import SimpleSku, Qty
from chalicelib.libs.purchase.core.dtd import Dtd
from chalicelib.libs.purchase.order.dtd_calculator import DtdCalculatorImplementation

products_blueprint = Blueprint(__name__)

# @todo : refactoring

# ----------------------------------------------------------------------------------------------------------------------
#                                                   PRODUCT
# ----------------------------------------------------------------------------------------------------------------------


def __create_search_criteria(params: Optional[dict]) -> ProductSearchCriteria:
    search_criteria = ProductSearchCriteria()

    if not params:
        return search_criteria

    page_number = int(params.get('pageNo') or 1)
    page_size = int(params.get('pageSize')) or None if params.get('pageSize') else None
    search_criteria.set_page(page_number, page_size)

    sort_column = params.get('sort') or ProductSearchCriteria.SORT_COLUMN_SCORE
    sort_direction = params.get('order') or ProductSearchCriteria.SORT_DIRECTION_ASC
    search_criteria.set_sort(sort_column, sort_direction)

    return search_criteria


def __get_tracking_info(config_sku_list: List[str]) -> dict:
    product_tracking_model = ProductsTrackingModel()
    user_id = products_blueprint.current_request.customer_id
    session_id = products_blueprint.current_request.session_id
    read_statuses = product_tracking_model.get_products_user_read_status(config_sku_list, user_id, session_id)

    counters = product_tracking_model.get_products_counters(config_sku_list)

    result = {}
    for config_sku in config_sku_list:
        result[config_sku] = {
            'views': counters[config_sku].get('views'),
            'visits': counters[config_sku].get('visits'),
            'clicks': counters[config_sku].get('clicks'),
            'is_read': read_statuses[config_sku],
        }

    return result


@products_blueprint.route('/list-products-by-filter', methods=['POST'], cors=True)
def list_products_by_filter():
    product = Product()
    scored_product = ScoredProduct()
    seen_app_service = SeenAppService()
    request: MPCRequest = products_blueprint.current_request
    current_user = request.current_user
    search_criteria = __create_search_criteria(request.query_params)
    user_id = current_user.user_id

    if not current_user.profile.is_personalized:
        status = scored_product.calculate_scores(current_user.email)
        if status:
            current_user.profile.is_personalized = True
        else:
            return {
                'total': 0,
                'products': []
            }

    response = scored_product.listByCustomFilter(
        user_id,
        request.json_body,
        [{
            search_criteria.sort_column: {"order": search_criteria.sort_direction}
        }],
        page=search_criteria.page_number,
        size=search_criteria.page_size,
        tier=current_user.profile.tier,
    )
    # Implemented scoring engine
    # response = product.listByCustomFilterWithPersonalize(
    #     request.email,
    #     request.json_body,
    #     [{
    #         search_criteria.sort_column: {"order": search_criteria.sort_direction}
    #     }],
    #     search_criteria.page_number,
    #     search_criteria.page_size
    # )

    # fbucks
    # tier = request.current_user.profile.tier
    # for product_data in response.get('products'):
    #     product_data['fbucks'] = None
    #     if not tier['is_neutral'] and not request.current_user.is_anyonimous:
    #         product_data['fbucks'] = math.ceil(product_data['current_price'] * tier['discount_rate'] / 100)

    # tracking info, is_seen
    # config_sku_list = [product_data.get('sku') for product_data in response.get('products')]
    # tracking_info = __get_tracking_info(config_sku_list)
    # if user_id is not None:
    #     seen_storage = seen_app_service.seen_storage(user_id)
    # for product_data in response.get('products'):
    #     config_sku = product_data.get('sku')
    #     product_data['tracking_info'] = tracking_info[config_sku]
    #     if user_id is not None:
    #         product_data['is_seen']=seen_storage.is_added(config_sku)

    # # filter only new
    # if user_id is not None and request.json_body['newin'] == "true":
    #     newin_response = {}
    #     newin_response['total'] = response['total'] - len(seen_storage.items)
    #     newin_response['products'] = []
    #     for product_data in response.get('products'):
    #         if product_data['is_seen'] == False:
    #             newin_response['products'].append(product_data)
    #     return newin_response

    return response


@products_blueprint.route('/get/{config_sku}', methods=['GET'], cors=True)
def get(config_sku):
    request = products_blueprint.current_request
    product = Product()

    response = product.get(
        config_sku,
        log=True,
        session_id=request.session_id,
        customer_id=request.customer_id
    )
    response['tracking_info'] = __get_tracking_info([config_sku])[config_sku]

    # @todo : refactoring
    original_price = float(response['rs_selling_price'])
    discount = float(response['discount'])
    current_price = original_price - original_price * discount / 100
    response['original_price'] = original_price
    response['current_price'] = current_price

    # fbucks
    tier = request.current_user.profile.tier
    response['fbucks'] = None
    if not tier['is_neutral'] and not request.current_user.is_anyonimous:
        response['fbucks'] = math.ceil(response['current_price'] * tier['discount_rate'] / 100)

    return response


# ----------------------------------------------------------------------------------------------------------------------
#                                                   DTD
# ----------------------------------------------------------------------------------------------------------------------


def __dtd_response(dtd: Dtd) -> dict:
    return {
        'occasion': {
            'name': dtd.occasion.name.value,
            'description': dtd.occasion.description.value,
        } if dtd.occasion else None,
        'date_from': dtd.date_from.strftime('%Y-%m-%d'),
        'date_to': dtd.date_to.strftime('%Y-%m-%d'),
        'working_days_from': dtd.working_days_from,
        'working_days_to': dtd.working_days_to,
    }


@products_blueprint.route('/calculate-dtd/default', methods=['GET'], cors=True)
def get_default_dtd():
    dtd_calculator = DtdCalculatorImplementation()
    default_dtd = dtd_calculator.get_default()
    return __dtd_response(default_dtd)


@products_blueprint.route('/calculate-dtd/{simple_sku}/{qty}', methods=['GET'], cors=True)
def get_dtd(simple_sku, qty):
    dtd_calculator = DtdCalculatorImplementation()

    try:
        simple_sku = SimpleSku(str(simple_sku or '').strip())
        qty = Qty(int(str(qty or 0).strip()))
    except (TypeError, ValueError):
        raise BadRequestError('Incorrect Input Data!')

    dtd = dtd_calculator.calculate(simple_sku, qty)
    return __dtd_response(dtd)


# ----------------------------------------------------------------------------------------------------------------------
#                                                   FILTER
# ----------------------------------------------------------------------------------------------------------------------


@products_blueprint.route('/available-filter', methods=['POST'], cors=True)
def available_filter():
    request = products_blueprint.current_request
    product = Product()
    sort = 'asc'
    if request.query_params is not None and request.query_params.get('sort') is not None:
        sort = request.query_params.get('sort')
    response = product.getAvailableFilter(request.json_body, sort)
    return response


@products_blueprint.route('/new-available-filter', methods=['POST'], cors=True)
def available_filter():
    request = products_blueprint.current_request
    product = Product()
    sort = 'asc'
    if request.query_params is not None and request.query_params.get('sort') is not None:
        sort = request.query_params.get('sort')
    response = product.getNewAvailableFilter(request.json_body, sort)
    return response

# ----------------------------------------------------------------------------------------------------------------------

