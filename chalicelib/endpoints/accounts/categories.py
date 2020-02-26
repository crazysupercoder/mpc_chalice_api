from typing import List
from ...libs.models.mpc.user import User
from ...libs.core.chalice.request import MPCRequest
from ...libs.models.mpc.categories import Category


def register_categories(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        return __get_request().current_user

    @blue_print.route('/categories', methods=['POST'], cors=True)
    def get_categories() -> List[dict]:
        user = __get_current_user()
        request = __get_request()
        category_model = Category()

        try:
            params = request.json_body
            genders = [user.gender] if params is None else params.get('genders', [user.gender])
        except Exception as e:
            print(str(e))
            genders = [user.gender]
        
        print(genders)

        saved_categories = user.profile.categories
        categories = category_model.get_categories(
            genders=genders,
            exclude=[item.id for item in saved_categories])
        results = dict()
        for category in categories:
            if results.get(category.gender_name) is None:
                results[category.gender_name] = []
            results[category.gender_name].append(category.to_dict())
        return results

    @blue_print.route('/categories/favorites', methods=['GET'], cors=True)
    def get_categories() -> List[dict]:
        user = __get_current_user()

        categories = user.profile.categories
        return [item.to_dict() for item in categories]

    @blue_print.route('/categories/favorites/{category_id}', methods=['POST', 'DELETE'], cors=True)
    def update_category(category_id) -> dict:
        user = __get_current_user()
        request = __get_request()
        if request.method == 'POST':
            # TODO: Add config_sku to favorite list for customer
            status = user.profile.add_category(category_id)
        elif request.method == 'DELETE':
            # TODO: Add config_sku to black list per customer
            status = user.profile.remove_category(category_id)

        return {"status": status}
