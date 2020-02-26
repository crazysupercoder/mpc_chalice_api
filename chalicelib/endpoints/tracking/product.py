from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.models.mpc.Product import Product as MpcProduct
from chalicelib.libs.models.mpc.product_tracking import (
    ViewAction, ClickAction, VisitAction, ProductsTrackingModel)
from chalicelib.libs.models.ml.scored_products import ScoredProduct


def register_product(blueprint: Blueprint):
    # ------------------------------------------------------------------------------------------------------------------
    #                                              TRACK VIEW
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/product/view', methods=['POST'], cors=True)
    def track_view():
        products_model = MpcProduct()
        # tracking_model = ProductsTrackingModel()

        # NEW VERSION
        scored_product = ScoredProduct()

        try:
            request_data = blueprint.current_request.json_body
            product_data = request_data.get('products')
            actions = list()

            if (
                not isinstance(product_data, list)
                or not product_data
                or sum([not (
                    (isinstance(product_item.get('config_sku'), str) and product_item.get('config_sku').strip())
                    or
                    (isinstance(product_item.get('position_on_page'), int) and product_item.get('position_on_page') > 0)
                ) for product_item in product_data]) > 0
            ):
                raise HttpIncorrectInputDataException()

            session_id = blueprint.current_request.session_id
            user_id = blueprint.current_request.customer_id
            user_tier_data = blueprint.current_request.current_user.profile.tier
            raw_products = products_model.get_raw_data_by_skus([
                item.get('config_sku') for item in product_data])
            raw_products = dict([(
                item['rs_sku'], item) for item in raw_products])

            for product_item in product_data:
                config_sku = product_item.get('config_sku')
                version = product_item.get('version')
                personalize_score = product_item.get('ps')
                question_score = product_item.get('qs')
                order_score = product_item.get('rs')
                tracking_score = product_item.get('ts')
                personalize_weight = product_item.get('pw')
                question_weight = product_item.get('qw')
                order_weight = product_item.get('rw')
                tracking_weight = product_item.get('tw')
                position_on_page = product_item.get('position_on_page')

                product_data = raw_products.get(config_sku)
                if not product_data:
                    continue

                actions.append(
                    ViewAction(
                        product_data,
                        position_on_page,
                        session_id,
                        user_id,
                        user_tier_data,
                        weight_version=version,
                        personalize_score=personalize_score,
                        personalize_weight=personalize_weight,
                        question_score=question_score,
                        question_weight=question_weight,
                        order_score=order_score,
                        order_weight=order_weight,
                        tracking_score=tracking_score,
                        tracking_weight=tracking_weight,
                    )
                )

            # tracking_model.track(actions)
            scored_product.track(actions)

            return {
                'Code': 'Success',
                'Message': 'Success',
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                              TRACK CLICK
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/product/click', methods=['POST'], cors=True)
    def track_click():
        products_model = MpcProduct()
        # tracking_model = ProductsTrackingModel()

        scored_product = ScoredProduct()

        try:
            request_data = blueprint.current_request.json_body
            config_sku = request_data.get('config_sku')
            weight_version = request_data.get('version')
            personalize_score = request_data.get('ps')
            question_score = request_data.get('qs')
            order_score = request_data.get('rs')
            tracking_score = request_data.get('ts')
            personalize_weight = request_data.get('pw')
            question_weight = request_data.get('qw')
            order_weight = request_data.get('rw')
            tracking_weight = request_data.get('tw')
            position_on_page = request_data.get('position_on_page')

            if not isinstance(config_sku, str) or not config_sku.strip():
                raise HttpIncorrectInputDataException('config_sku is incorrect')

            if not isinstance(position_on_page, int) or position_on_page < 1:
                raise HttpIncorrectInputDataException('position_on_page >= 1')

            product_data = products_model.get_raw_data(config_sku)
            if not product_data:
                raise HttpNotFoundException()

            session_id = blueprint.current_request.session_id
            user_id = blueprint.current_request.customer_id
            user_tier_data = blueprint.current_request.current_user.profile.tier

            scored_product.track(ClickAction(
                product_data,
                position_on_page,
                session_id,
                user_id,
                user_tier_data,
                weight_version=weight_version,
                personalize_score=personalize_score,
                personalize_weight=personalize_weight,
                question_score=question_score,
                question_weight=question_weight,
                order_score=order_score,
                order_weight=order_weight,
                tracking_score=tracking_score,
                tracking_weight=tracking_weight,
            ))

            return {
                'Code': 'Success',
                'Message': 'Success',
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                              TRACK VISIT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/product/visit', methods=['POST'], cors=True)
    def track_visit():
        products_model = MpcProduct()
        # tracking_model = ProductsTrackingModel()

        scored_product = ScoredProduct()

        try:
            request_data = blueprint.current_request.json_body
            config_sku = request_data.get('config_sku')
            weight_version = request_data.get('version')
            personalize_score = request_data.get('ps')
            question_score = request_data.get('qs')
            order_score = request_data.get('rs')
            tracking_score = request_data.get('ts')
            personalize_weight = request_data.get('pw')
            question_weight = request_data.get('qw')
            order_weight = request_data.get('rw')
            tracking_weight = request_data.get('tw')

            if not isinstance(config_sku, str) or not config_sku.strip():
                raise HttpIncorrectInputDataException('config_sku is incorrect')

            product_data = products_model.get_raw_data(config_sku)
            if not product_data:
                raise HttpNotFoundException()

            session_id = blueprint.current_request.session_id
            user_id = blueprint.current_request.customer_id
            user_tier_data = blueprint.current_request.current_user.profile.tier

            scored_product.track(VisitAction(
                product_data,
                session_id,
                user_id,
                user_tier_data,
                weight_version=weight_version,
                personalize_score=personalize_score,
                personalize_weight=personalize_weight,
                question_score=question_score,
                question_weight=question_weight,
                order_score=order_score,
                order_weight=order_weight,
                tracking_score=tracking_score,
                tracking_weight=tracking_weight,
            ))

            # Send message to SQS so that recalculate scores
            blueprint.current_request.current_user.send_delta_cache_update_message_to_sqs()

            return {
                'Code': 'Success',
                'Message': 'Success',
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

