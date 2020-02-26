from typing import List, Tuple
from ..mpc.user import User
from ..mpc.Cms.profiles import Profile
from ..mpc.product_tracking import ProductsTrackingModel
from .questions import Answer
from .products import ProductEntry, Product
from .orders import Order, OrderAggregation
from .tracks import UserTrackEntry
from .utils import get_bucket_data
from .delta_cache import DeltaCache
from .weights import ScoringWeight


class DeltaBucketToolkit(object):
    __cache = DeltaCache()

    @classmethod
    def calculate_questions_scores(
            cls,
            products: List[ProductEntry],
            questions: List[Answer]) -> List[ProductEntry]:
        for product in products:
            product.apply_questions(questions)
        return products

    @classmethod
    def calculate_order_scores(
            cls,
            products: List[ProductEntry],
            orders: OrderAggregation) -> List[ProductEntry]:
        for product in products:
            product.apply_orders(orders)
        return products

    @classmethod
    def calculate_tracking_scores(
            cls,
            products: List[ProductEntry],
            tracking_data: UserTrackEntry,
            **kwargs) -> List[ProductEntry]:
        for product in products:
            product.apply_trackings(tracking_data)
        return products

    @classmethod
    def get_buckets_with_email(
            cls,
            email: str,
            size: int = 500,
            username: str = None,
            weight: ScoringWeight = None,
            sort: bool = True,
            **kwargs) -> List[ProductEntry]:
        # TODO: Check delta cache
        cached_products = cls.__cache.get(email)
        username, products = get_bucket_data(
            email, size=size, cached=cached_products,
            username=username)

        # Getting tracking data for the given customer
        # UPDATED THIS RECENTLY
        if username:
            model = ProductsTrackingModel()
            trackings = model.get_visited_products_aggregation_data(username)
            products = cls.calculate_tracking_scores(
                products,
                UserTrackEntry(len(products), **trackings))

        if weight is None:
            print("REALLY?")
            weight = ScoringWeight()
        # products = sorted(
        #     products,
        #     key=lambda product: product.total_score, reverse=True)
        if sort:
            products = sorted(
                products,
                # key=lambda product: product.weighted_total_score(**weight.to_shorten_dict()), reverse=True)
                key=lambda product: product.total_score, reverse=True)

        return products
