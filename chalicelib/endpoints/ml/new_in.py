from ...libs.models.ml.products import Product
from ...libs.models.mpc.user import User
from ...libs.core.chalice.request import MPCRequest


def register_new_ins(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/new_in', cors=True)
    def new_in(email=''):
        request = __get_request()
        product = Product()
        products = product.get_new_products(
            page=request.page, size=request.size,
            customer_id=request.current_user.email,
            gender=request.gender)
        return products

    @blue_print.route('/last_chance', cors=True)
    def last_chance():
        request = __get_request()
        product = Product()
        product_types = product.get_last_chance(
            page=request.page, size=request.size,
            customer_id=request.current_user.email,
            gender=request.gender)
        return product_types

    @blue_print.route('/bestsellers', cors=True)
    def best_sellers():
        request = blue_print.current_request
        product = Product()
        best_sellers = product.get_bestsellers(page=request.page, size=request.size)
        return best_sellers

    @blue_print.route('/bestsellers/{product_type_name}', cors=True)
    def best_sellers(product_type_name):
        request = blue_print.current_request
        product = Product()
        best_sellers = product.get_bestsellers(
            product_type=product_type_name, page=request.page, size=request.size)
        return best_sellers
