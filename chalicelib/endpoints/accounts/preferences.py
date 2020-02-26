from typing import List
from ...libs.models.mpc.user import User
from ...libs.core.chalice.request import MPCRequest


def register_preferences(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/preferences', methods=['GET', 'POST'], cors=True)
    def get_preferences() -> List[dict]:
        user = __get_current_user()
        request = __get_request()
        if request.method == 'GET':
            preference = user.profile.preference
            return preference.to_dict()
        elif request.method == 'POST':
            params = request.json_body
            try:
                user.profile.preference = params
                return {'status': True}
            except Exception as e:
                return {'status': False, 'msg': str(e)}
