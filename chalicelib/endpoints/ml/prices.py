from ...libs.models.ml.products import Product


def register_prices(blue_print):
    @blue_print.route('/shop_by_price', cors=True)
    def shop_by_price():
        product = Product()
        request = blue_print.current_request
        min_price = 0 if request.query_params is None else int(request.query_params.get('min', 0))
        max_price = 200 if request.query_params is None else int(request.query_params.get('max', 200))
        products = product.get_by_price(
            max_price, min_price=min_price, page=request.page, size=request.size)

        return products
