from chalice import Blueprint
from chalicelib.libs.models.mpc.Admin.Magento import Magento
from chalicelib.libs.purchase.cart.service import CartAppService


magento_blueprint = Blueprint(__name__)


@magento_blueprint.route('/validate', methods=['POST'], cors=True)
def validate_user():
    request = magento_blueprint.current_request
    params = request.json_body
    email = params.get('email')
    passwd_to_validate = params.get('password')

    mage = Magento()
    found, created, msg = mage.validate_user(email, passwd_to_validate)

    return {
        'created': created,
        'found': found,
        'message': msg
    }


@magento_blueprint.route('/sync', methods=['POST'], cors=True)
def sync_user_data():
    request = magento_blueprint.current_request
    user_attributes = request.current_user.profile.sync_user_data()
    response = request.current_user.sync_user_attributes(user_attributes)

    return {
        'status': response
    }


# @todo : move login and logout actions from the frontend side, and then remove on-* endpoints

# ----------------------------------------------------------------------------------------------------------------------


@magento_blueprint.route('/on-login', methods=['POST'], cors=True)
def on_login():
    session_id = magento_blueprint.current_request.current_user.session_id
    user_id = magento_blueprint.current_request.current_user.id

    cart_app_service = CartAppService()
    cart_app_service.transfer_cart(session_id, user_id)


@magento_blueprint.route('/on-logout', methods=['DELETE'], cors=True)
def on_logout():
    pass


# ----------------------------------------------------------------------------------------------------------------------

