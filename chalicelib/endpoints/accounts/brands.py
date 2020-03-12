from typing import List
from urllib.parse import unquote
from ...libs.core.chalice.request import MPCRequest
from ...libs.models.mpc.user import User
from chalicelib.libs.models.ml.scored_products import ScoredProduct
from ...libs.models.ml.products import Product


def register_brands(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/brands', methods=['GET'], cors=True)
    def brands() -> List[dict]:
        product_model = Product()
        request = __get_request()

        prefix = request.get_query_parameter('prefix', 'a+b+c').split(' ')
        brands = product_model.get_brands(
            prefix=prefix, exclude=request.current_user.profile.brands,
            page=request.page, size=request.size)
        return brands

    @blue_print.route('/brands/favorite', methods=['GET'], cors=True)
    def brands() -> List[dict]:
        user = __get_current_user()
        return user.profile.brands

    @blue_print.route('/brands/popular', methods=['GET'], cors=True)
    def brands() -> List[dict]:
        request: MPCRequest = __get_request()
        user = request.current_user

        brands = ScoredProduct().get_top_brands(
            user.id, exclude=user.profile.brands,
            page=request.page, size=request.size,
        )
        return brands

    @blue_print.route('/brands/{brand_name}', methods=['POST', 'DELETE'], cors=True)
    def update_accounts_brand(brand_name) -> dict:
        request = __get_request()
        brand_name = unquote(brand_name)
        if request.method == 'POST':
            status = request.current_user.profile.add_brand(brand_name)
        elif request.method == 'DELETE':
            status = request.current_user.profile.remove_brand(brand_name)
        return {'status': status}
