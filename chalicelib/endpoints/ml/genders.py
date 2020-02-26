from ...libs.core.personalize import GenderPersonalize


def register_gender(blue_print):
    @blue_print.route('/gender', cors=True)
    def product_gender():
        request = blue_print.current_request
        gender = request.current_user.gender
        return {
            "data": gender
        }

    @blue_print.route('/gender/metrics', cors=True)
    def gender_metrics():
        return GenderPersonalize.get_metrics()
