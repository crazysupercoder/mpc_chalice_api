from chalicelib.libs.core.chalice import MPCRequest
from ...libs.models.ml.scored_products import ScoredProduct


def register_sizes(blue_print):
    @blue_print.route('/category_by_size', cors=True)
    def get_by_size():
        request: MPCRequest = blue_print.current_request
        product_model = ScoredProduct()

        product_type_name = None
        product_size_name = None
        # TODO: Logic to get size saved by user in profile.
        if len(request.current_user.profile.product_types) > 0:
            product_type_name = request.current_user.profile.product_types[0]

        if product_type_name is None:
            categories = product_model.get_categories_by_gender(
                customer_id=request.current_user.id,
                gender=request.gender, size=1)
            if len(categories) > 0:
                product_type_name = response[0]['product_type_name']
            else:
                return {
                    'product_type': product_type_name,
                    'product_size': None,
                    'products': []
                }

        if product_size_name is None:
            candidates = product_model.get_sizes_by_product_type(
                product_type_name, request.gender,
                customer_id=request.current_user.id)
            if len(candidates) == 0:
                return {
                    'product_type': product_type_name,
                    'product_size': None,
                    'products': []
                }
            else:
                product_size_name = candidates[0]

        products = product_model.get_by_size(
            product_size_name, product_type=product_type_name, gender=request.gender,
            page=request.page, size=request.size)

        return {
            'product_type': product_type_name,
            'product_size': product_size_name,
            'products': products
        }
