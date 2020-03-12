from typing import List
from chalice import ForbiddenError
from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.models.ml.scored_products import ScoredProduct
from ...libs.models.mpc.user import User
from ...libs.models.mpc.Cms.meta import Meta
from ...libs.models.mpc.Cms.weight import WeightModel
from ...libs.models.ml.products import Product
from ...libs.seen.service import SeenAppService


def register_products(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/products', cors=True)
    def products():
        request = __get_request()
        current_user = request.current_user

        response = ScoredProduct().listByCustomFilter(
            customer_id=current_user.id,
            sort_by_score=True,
            tier=current_user.profile.tier,
            page=request.page, size=request.size)

        return response['products']  # response

    @blue_print.route('/admin/product_scoring', cors=True, methods=['GET', 'POST'])
    def products():
        request = __get_request()
        current_user = request.current_user
        weight = WeightModel()

        if not current_user.is_admin:
            raise ForbiddenError('Administrators are only permitted!')
        if request.method == 'POST':
            weight.scoring_weight = request.json_body
        return weight.scoring_weight.to_dict(to_str=True)

    @blue_print.route('/admin/report', cors=True, methods=['POST'])
    def products():
        request = __get_request()
        current_user = request.current_user

        if not current_user.is_admin:
            raise ForbiddenError('Administrators are only permitted!')

        return {"status": "OK"}

    @blue_print.route('/admin/{secret_key}/products/{email}', cors=True)
    def products(secret_key: str, email: str):
        request = __get_request()
        current_user = request.current_user
        meta = Meta()
        weight = WeightModel()

        if not current_user.is_admin:
            raise ForbiddenError('Administrators are only permitted!')
        elif not meta.check_secret_key(secret_key):
            raise ForbiddenError('Invalid Secret Key.')

        response = ScoredProduct().listByCustomFilter(
            email=email, sort_by_score=True,
            tier=current_user.profile.tier,
            page=request.page, size=request.size)

        return response['products']  # response

    @blue_print.route('/products/{product_id}', cors=True)
    def get_product(product_id):
        request = __get_request()
        product = Product()
        item = product.find_by_id(
            product_id, session_id=request.session_id,
            user_id=request.current_user.email, log=True,
            tier=request.current_user.profile.tier)
        return item

    @blue_print.route('/products/{product_id}/complete_looks', cors=True)
    def get_product(product_id):
        seen_app_service = SeenAppService()
        request = __get_request()
        user_id = request.customer_id
        response = ScoredProduct().get_complete_looks(
            product_id, customer_id=user_id,
            size=request.size, page=request.page,
            tier=request.current_user.profile.tier)

        return response

    @blue_print.route('/products/{product_id}/similar_styles', cors=True)
    def get_product(product_id):
        request = __get_request()
        product = Product()
        response = product.get_smiliar_styles(
            product_id, customer_id=request.current_user.email,
            size=request.size, page=request.page,
            tier=request.current_user.profile.tier)
        return response

    @blue_print.route('/products/{product_id}/also_availables', cors=True)
    def get_product(product_id):
        request = __get_request()
        product = Product()
        item = product.get_also_availables(
            product_id, tier=request.current_user.profile.tier)
        return item

    @blue_print.route('/products/{product_id}/recently_views', cors=True)
    def get_product(product_id):
        request = __get_request()
        product = Product()
        response = product.get_recently_viewed(
            request.session_id,
            customer_id=request.customer_id,
            product_id=product_id,
            tier=request.current_user.profile.tier)
        return response
