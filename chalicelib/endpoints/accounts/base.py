from chalice import Blueprint
from .questions import register_questions
from .brands import register_brands
from .preferences import register_preferences
from .informations import register_informations
from .categories import register_categories
from .sizes import register_sizes


accounts_blueprint = Blueprint(__name__)
register_questions(accounts_blueprint)
register_brands(accounts_blueprint)
register_preferences(accounts_blueprint)
register_informations(accounts_blueprint)
register_categories(accounts_blueprint)
register_sizes(accounts_blueprint)
