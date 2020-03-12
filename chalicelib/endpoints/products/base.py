import math
from typing import Optional
from chalice import Blueprint, BadRequestError
from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.models.mpc.Product import Product, ProductSearchCriteria
from chalicelib.libs.models.ml.scored_products import ScoredProduct
from chalicelib.libs.purchase.core import SimpleSku, Qty, Dtd
from chalicelib.libs.purchase.order.dtd_calculator import DtdCalculatorImplementation

products_blueprint = Blueprint(__name__)

# @todo : refactoring

# ----------------------------------------------------------------------------------------------------------------------
#                                                   PRODUCT
# ----------------------------------------------------------------------------------------------------------------------


def __create_search_criteria(params: Optional[dict], is_personalized: bool) -> ProductSearchCriteria:
    search_criteria = ProductSearchCriteria()

    if not params:
        return search_criteria

    page_number = int(params.get('pageNo') or 1)
    page_size = int(params.get('pageSize')) or None if params.get('pageSize') else None
    search_criteria.set_page(page_number, page_size)

    if is_personalized:
        sort_column = params.get('sort') or ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE
        sort_direction = params.get('order') or ProductSearchCriteria.SORT_DIRECTION_DESC
    else:
        sort_column = params.get('sort') or ProductSearchCriteria.SORT_COLUMN_SCORE
        sort_direction = params.get('order') or ProductSearchCriteria.SORT_DIRECTION_ASC

    search_criteria.set_sort(sort_column, sort_direction)

    return search_criteria


@products_blueprint.route('/list-products-by-filter', methods=['POST'], cors=True)
def list_products_by_filter():
    product = Product()
    scored_product = ScoredProduct()
    request: MPCRequest = products_blueprint.current_request
    current_user = request.current_user
    search_criteria = __create_search_criteria(request.query_params, current_user.is_personalized)
    user_id = current_user.user_id

    if current_user.is_personalized:
        response = scored_product.listByCustomFilter(
            customer_id=user_id,
            custom_filters=request.json_body,
            sorts={
                search_criteria.sort_column: search_criteria.sort_direction
            },
            page=search_criteria.page_number,
            size=search_criteria.page_size,
            tier=current_user.profile.tier,
        )
    else:
        if not current_user.is_anyonimous:
            current_user.send_calculate_product_score_for_customers([current_user.email])

        response = product.listByCustomFilter(
            request.json_body,
            sorts={
                search_criteria.sort_column: search_criteria.sort_direction
            },
            page=search_criteria.page_number,
            size=search_criteria.page_size,
            tier=current_user.profile.tier,
        )

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

