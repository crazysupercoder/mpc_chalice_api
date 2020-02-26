from ...libs.core.personalize import ConfigSkuPersonalize


def register_config_skus(blue_print):
    @blue_print.route('/config_skus', cors=True)
    def config_skus():
        request = blue_print.current_request
        response = ConfigSkuPersonalize.get_recommend_ids(
            size=request.size, customer_id=request.current_user.email)
        return response

    @blue_print.route('/config_skus/metrics', cors=True)
    def product_type_metrics():
        return ConfigSkuPersonalize.get_metrics()


    @blue_print.route('/config_skus/{item_id}/similar/', cors=True)
    def config_skus(item_id):
        request = blue_print.current_request
        response = ConfigSkuPersonalize.get_similar_items(
            item_id, size=request.size, customer_id=request.current_user.email)
        return response

    @blue_print.route('/config_skus/similar/metrics', cors=True)
    def product_type_metrics():
        return ConfigSkuPersonalize.get_similar_items_solution_metrics()
