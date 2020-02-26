from ...libs.core.personalize import SimpleSkuPersonalize


def get_simple_sku_recommendations(email='BLANK', size=10):
    product_simple_skus = SimpleSkuPersonalize.get_recommend_ids(
        customer_id=email, size=size)
    return product_simple_skus


def register_simple_skus(blue_print):
    @blue_print.route('/simple_skus', cors=True)
    def simple_skus(cors=True):
        response = get_simple_sku_recommendations()
        return response


    @blue_print.route('/simple_skus/{email}', cors=True)
    def simple_skus(email='', cors=True):
        response = get_simple_sku_recommendations(email)
        return response
