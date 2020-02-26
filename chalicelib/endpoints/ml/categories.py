from ...libs.core.personalize import ProductTypePersonalize
from ...libs.models.mpc.categories import Category


def register_categories(blue_print):
    @blue_print.route('/categories', cors=True)
    def product_types():
        request = blue_print.current_request
        category_model = Category()
        response = category_model.get_categories_by_gender(
            request.gender, customer_id=request.current_user.email,
            user_defined_product_types=request.current_user.profile.product_types)
        return response

    @blue_print.route('/categories/sub_types', cors=True)
    def category_by_subtypes():
        request = blue_print.current_request
        if len(request.current_user.profile.product_types) > 0:
            response = ProductTypePersonalize.get_subtypes(
                customer_id=request.current_user.email, gender=request.gender,
                product_type_name=request.current_user.profile.product_types[0])
        else:
            response = ProductTypePersonalize.get_subtypes(
                customer_id=request.current_user.email, gender=request.gender)
        return response

    @blue_print.route('/categories/{category_id}/products', cors=True)
    def category_by_subtypes(category_id):
        request = blue_print.current_request
        category_model = Category()
        response = category_model.get_products_by_id(
            category_id, page=request.page, size=request.size)
        return response

    @blue_print.route('/categories/metrics', cors=True)
    def product_type_metrics():
        return ProductTypePersonalize.get_metrics()
