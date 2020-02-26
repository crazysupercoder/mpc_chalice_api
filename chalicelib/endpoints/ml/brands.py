from ...libs.core.personalize import ProductBrandPersonalize
from ...libs.models.ml.products import Product


def register_brands(blue_print):
    @blue_print.route('/brands', cors=True)
    def product_brands():
        product_model = Product()
        request = blue_print.current_request
        brands = request.current_user.profile.brands
        response = product_model.get_top_brands(
            size=request.size, customer_id=request.email,
            user_defined=brands)
        return response

    @blue_print.route('/brands/metrics')
    def brand_metrics():
        return ProductBrandPersonalize.get_metrics()