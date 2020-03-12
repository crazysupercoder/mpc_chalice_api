import boto3
from typing import List, Tuple
from chalicelib.libs.models.mpc.Product import Product, ProductEntry
from ..mpc.user import User
from ..mpc.Cms.profiles import Profile
from ..mpc.Cms.weight import WeightModel
from .orders import Order, OrderAggregation
from .questions import Answer


def get_username_from_email(email: str) -> str:
    return User.get_username_with_email(email)


def get_email_from_username(username: str) -> str:
    __data = User.find_user(username)
    for item in __data.get('UserAttributes', []):
        if item.get('Name') == 'email':
            return item.get('Value')


def get_bucket_data(
            email: str,
            size: int = 500,
            username: str = None,
            **kwargs
        ) -> Tuple[str, List[ProductEntry]]:
    if email and not username:
        username = User.get_username_with_email(email)
    product_model = Product()
    weight_model = WeightModel()
    weights = weight_model.scoring_weight

    min_order_score = 0

    products = product_model.get_all()
    
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
