from chalice import Blueprint, BadRequestError
from chalicelib.libs.models.mpc.Product import Product

blueprint = Blueprint(__name__)


@blueprint.route('suggest/{query}', methods=['GET'], cors=True)
def suggest(query):
    query = str(query or '').strip()
    if len(query) < 3:
        raise BadRequestError('"query" length must be >= 3!')

    # @todo : update criteria and use it
    products = Product().listByCustomFilter(
        {'search_query': query},
        {"_score": "asc"},
        blueprint.current_request.current_user.profile.tier,
        1,
        10000
    ).get('products')

    return {
        'query': query,
        'products': [{
            'sku': product.get('sku'),
            'name': product.get('title'),
            'description': product.get('subtitle'),
            'brand_name': product.get('brand'),
            'image_src': product.get('image').get('src'),
            'original_price': float(product.get('original_price')),
            'current_price': float(product.get('current_price')),
        } for product in products],
        'brands': tuple(set([product.get('brand') for product in products]))
    }

