from typing import List
from ...libs.models.mpc.user import User
from ...libs.core.chalice.request import MPCRequest
from ...libs.core.personalize import ConfigSkuPersonalize
from ...libs.models.ml.products import Product


def register_recommendations(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/recommendations', methods=['GET'], cors=True)
    def get_recommendations() -> List[dict]:
        user = __get_current_user()
        request = __get_request()
        product_model = Product()

        dislikes = user.profile.dislikes
        response = product_model.get_recommends(
            size=request.size, customer_id=user.email,
            exclude=dislikes)
        return response

    @blue_print.route('/recommendations/{config_sku}', methods=['POST', 'DELETE'], cors=True)
    def get_recommendations(config_sku) -> List[dict]:
        user = __get_current_user()
        request = __get_request()
        product_model = Product()

        if request.method == 'POST':
            # TODO: Add config_sku to favorite list for customer
            like = ConfigSkuPersonalize.track_event(
                user.email, user.session_id, config_sku,
                event_type='like', value='true')
            user.profile.set_like(config_sku)
        elif request.method == 'DELETE':
            # TODO: Add config_sku to black list per customer
            dislike = ConfigSkuPersonalize.track_event(
                user.email, user.session_id, config_sku,
                event_type='like', value='false')
            user.profile.set_dislike(config_sku)

        response = product_model.get_recommends(
            size=request.size, customer_id=user.email,
            exclude=user.profile.dislikes)
        return response
