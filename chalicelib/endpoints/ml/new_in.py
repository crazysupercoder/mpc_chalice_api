
from ...libs.core.chalice.request import MPCRequest
from ...libs.models.ml.scored_products import ScoredProduct
from ...libs.models.mpc.user import User


def register_new_ins(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/new_in', cors=True)
    def new_in(email=''):
        request = __get_request()
        product = ScoredProduct()
        response = product.get_new_products(
            customer_id=request.current_user.id,
            gender=request.gender,
            tier=request.current_user.profile.tier,
            page=request.page, size=request.size)
        return response['products']

    @blue_print.route('/last_chance', cors=True)
    def last_chance():
        request = __get_request()
        product_types = ScoredProduct().get_last_chance(
            customer_id=request.current_user.id,
            page=request.page, size=request.size,
            gender=request.gender)
        return product_types
