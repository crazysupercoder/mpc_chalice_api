import boto3
from typing import List, Tuple
from ..mpc.user import User
from ..mpc.Cms.profiles import Profile
from ..mpc.Cms.weight import WeightModel
from .products import Product, ProductEntry
from .orders import Order, OrderAggregation
from .questions import Answer


def get_bucket_data(
            email: str,
            size: int = 500,
            cached: list = [],
            username: str = None,
            **kwargs
        ) -> Tuple[str, List[ProductEntry]]:
    if email and not username:
        username = User.get_username_with_email(email)
    product_model = Product()
    weight_model = WeightModel()
    weights = weight_model.scoring_weight

    min_order_score = 0

    if isinstance(cached, list) and len(cached) > 0:
        products = product_model.get_by_skus([item.rs_sku for item in cached])
        for product in products:
            filtered = [item for item in cached if product.rs_sku == item.rs_sku]
            if len(filtered) == 0:
                raise Exception("Unexpected case found.")
            cached_item = filtered[0]
            product.question_score = cached_item.question_score
            if min_order_score > cached_item.order_score:
                min_order_score = cached_item.order_score
            product.order_score = cached_item.order_score
            product.personalize_score = cached_item.personalize_score
            product.tracking_score = cached_item.tracking_score
            product.set_weights(weights)
        omitted = product_model.get_products_exclude_config_skus([item.rs_sku for item in cached])
        for product in omitted:
            product.order_score = min_order_score
        products += omitted
    else:
        products = product_model.get_delta_list(
            email=email or 'BLANK', size=size)
        
        if username is not None:
            order_model = Order()
            orders = order_model.get_order_aggregation(email)

            # TODO: preprecessing answers to filter proper answers
            answers = [
                Answer(product_count=len(products), **item.get('data', {}))
                for item in Profile.get_answers_by_customer(username)]
            valid_answers = [answer for answer in answers if answer.target_attr is not None]
        else:
            orders = OrderAggregation(product_count=size)
            valid_answers = []
        
        for product in products:
            product.apply_questions(valid_answers)
            product.apply_orders(orders)
            product.set_weights(weights)
    return username, products
