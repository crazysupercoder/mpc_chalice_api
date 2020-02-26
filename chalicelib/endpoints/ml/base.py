from chalice import Blueprint
from .categories import register_categories
from .brands import register_brands
from .config_skus import register_config_skus
from .prices import register_prices
# from .simple_skus import register_simple_skus
from .sizes import register_sizes
from .new_in import register_new_ins
from .products import register_products
from .genders import register_gender
from .events import register_event_trackers


ml_blueprint = Blueprint(__name__)
register_categories(ml_blueprint)
register_brands(ml_blueprint)
register_config_skus(ml_blueprint)
# register_simple_skus(blue_print)
register_sizes(ml_blueprint)
register_prices(ml_blueprint)
register_new_ins(ml_blueprint)
register_products(ml_blueprint)
register_gender(ml_blueprint)
register_event_trackers(ml_blueprint)
