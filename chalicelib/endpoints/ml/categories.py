from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.models.ml.scored_products import ScoredProduct
from ...libs.models.mpc.categories import Category


def register_categories(blue_print):
    @blue_print.route('/categories', cors=True)
    def product_types():
        request: MPCRequest = blue_print.current_request
        response = ScoredProduct().get_categories_by_gender(
            request.gender, customer_id=request.current_user.id,
            user_defined_product_types=request.current_user.profile.product_types)
        return response

    @blue_print.route('/categories/{category_id}/products', cors=True)
    def category_by_subtypes(category_id):
        request = blue_print.current_request
        category_model = Category()
        response = category_model.get_products_by_id(
            category_id, page=request.page, size=request.size)
        return response
