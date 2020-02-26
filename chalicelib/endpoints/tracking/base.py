from chalice import Blueprint
from .product import register_product

blueprint = Blueprint(__name__)

register_product(blueprint)
