from chalice import Blueprint
from chalicelib.libs.models.mpc.invite_friends import InviteFriends
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.extensions import *

invite_friends_blueprint = Blueprint(__name__)

def __get_request() -> MPCRequest:
    return invite_friends_blueprint.current_request

def __get_current_user() -> User:
    user = __get_request().current_user
    if user.is_anyonimous:
        raise HttpAuthenticationRequiredException()

    return user
    
@invite_friends_blueprint.route('/send_invitation', methods=['POST'], cors=True)
def validate_user():
    # user = __get_current_user()
    # request = __get_request()
    # to_email = request.json_body['email']
    # invite_friends = InviteFriends(user.profile.informations['first_name'])
    # invite_friends.send_invite_email(to_email)
    # return {'status': True}
    try:
        user = __get_current_user()
        request = __get_request()
        to_email = request.json_body['email']
        invite_friends = InviteFriends(user.profile.informations['first_name'])
        invite_friends.send_invite_email(to_email)
        return {'status': True}
    except:
        return {'status': False}


