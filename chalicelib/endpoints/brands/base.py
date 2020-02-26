from chalice import Blueprint
from chalicelib.libs.models.mpc.brands import Brand

brands_blueprint = Blueprint(__name__)

@brands_blueprint.route('/list-brands', methods=['GET'], cors=True)
def list_brands():
    brands = Brand()
    return brands.get_all_brands()
