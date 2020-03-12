from chalice import Blueprint
from .categories import register_categories
from .brands import register_brands
from .prices import register_prices
from .sizes import register_sizes
from .new_in import register_new_ins
from .products import register_products
from .genders import register_gender


ml_blueprint = Blueprint(__name__)
register_categories(ml_blueprint)
register_brands(ml_blueprint)
register_sizes(ml_blueprint)
register_prices(ml_blueprint)
register_new_ins(ml_blueprint)
register_products(ml_blueprint)
register_gender(ml_blueprint)
