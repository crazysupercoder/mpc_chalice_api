from chalicelib.libs.models.ml.scored_products import ScoredProduct


def register_brands(blue_print):
    @blue_print.route('/brands', cors=True)
    def product_brands():
        request = blue_print.current_request
        brands = request.current_user.profile.brands
        response = ScoredProduct().get_top_brands(
            request.user.id, user_defined=brands,
            size=request.size, customer_id=request.email,)
        return response
