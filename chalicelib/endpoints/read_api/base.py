from chalice import Blueprint
from .orders import register_orders
from .returns import register_returns

blueprint = Blueprint(__name__)

register_orders(blueprint)
register_returns(blueprint)
