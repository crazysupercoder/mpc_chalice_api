from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.core.logger import Logger


def register_sizes(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    def __response_success(data: dict) -> dict:
        return {
            'status': True,
            'data': data,
        }

    def __response_error(error_message: str) -> dict:
        return {
            'status': False,
            'error_message': error_message
        }

    @blue_print.route('/sizes', methods=['GET', 'POST'], cors=True)
    def brands() -> dict:
        request = __get_request()
        current_user = __get_current_user()
        if request.method == 'GET':
            sizes = current_user.profile.sizes
            return __response_success(sizes.to_dict())
        elif request.method == 'POST':
            params = request.json_body
            current_user.profile.sizes = params
            return __response_success(current_user.profile.sizes.to_dict())

    @blue_print.route('/possible-favorite-sizes', methods=['GET'], cors=True)
    def possible_favorite_sizes():
        user = __get_current_user()
        logger = Logger()
        if user.is_anyonimous:
            return __response_error('Authentication is required!')

        try:
            response = {}
            for category in user.profile.categories:
                gender_key = category.gender_name
                type_key = category.product_type_name

                response[gender_key] = response.get(gender_key) or {}
                response[gender_key][type_key] = response[gender_key].get(type_key) or []
                response[gender_key][type_key].extend([size.name for size in category.sizes])
                response[gender_key][type_key] = list(set(response[gender_key][type_key]))

            return __response_success(response)
        except BaseException as e:
            logger.log_exception(e)
            return __response_error('Internal Server Error')

