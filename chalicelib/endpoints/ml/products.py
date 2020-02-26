from typing import List
from chalice import ForbiddenError
from ...libs.core.chalice.request import MPCRequest
from ...libs.models.mpc.user import User
from ...libs.models.mpc.Cms.meta import Meta
from ...libs.models.mpc.Cms.weight import WeightModel
from ...libs.models.ml.products import Product
from ...libs.models.ml.delta import DeltaBucketToolkit
from ...libs.models.mpc.product_tracking import ProductsTrackingModel
from ...libs.seen.service import SeenAppService


def register_products(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    def __get_tracking_info(config_sku_list: List[str]) -> dict:
        product_tracking_model = ProductsTrackingModel()
        current_request = __get_request()
        user_id = current_request.customer_id
        session_id = current_request.session_id
        read_statuses = product_tracking_model.get_products_user_read_status(config_sku_list, user_id, session_id)

        counters = product_tracking_model.get_products_counters(config_sku_list)

        result = {}
        for config_sku in config_sku_list:
            result[config_sku] = {
                'views': counters[config_sku].get('views'),
                'visits': counters[config_sku].get('visits'),
                'clicks': counters[config_sku].get('clicks'),
                'is_read': read_statuses[config_sku],
            }

        return result

    @blue_print.route('/products', cors=True)
    def products():
        request = __get_request()
        current_user = request.current_user
        weight = WeightModel()

        products = DeltaBucketToolkit.get_buckets_with_email(
            current_user.email, username=current_user.user_id,
            weight=weight.scoring_weight, size=500)
        return [item.to_dict(mode='list') for item in products]

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
        products = DeltaBucketToolkit.get_buckets_with_email(
            email, weight=weight.scoring_weight, size=500)
        return [item.to_dict(mode='list') for item in products]

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
        product = Product()
        response = product.get_complete_looks(
            product_id, customer_id=request.current_user.email,
            size=request.size, page=request.page,
            tier=request.current_user.profile.tier)
        
        # tracking info, is_seen
        user_id = request.customer_id
        config_sku_list = [product_data.get('sku') for product_data in response]
        tracking_info = __get_tracking_info(config_sku_list)
        if user_id is not None:
            seen_storage = seen_app_service.seen_storage(user_id)
        for product_data in response:
            config_sku = product_data.get('sku')
            product_data['tracking_info'] = tracking_info[config_sku]
            if user_id is not None:
                product_data['is_seen']=seen_storage.is_added(config_sku)

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
