from ...libs.models.ml.products import Product
from ...libs.core.personalize import ProductSizePersonalize, ProductTypePersonalize


def register_sizes(blue_print):
    # @blue_print.route('/sizes', cors=True)
    # def sizes():
    #     request = blue_print.current_request
    #     response = ProductSizePersonalize.get_recommends(
    #         customer_id=request.current_user.email, size=request.size)
    #     return response

    @blue_print.route('/category_by_size', cors=True)
    def get_by_size():
        request = blue_print.current_request
        product_model = Product()

        product_type_name = None
        product_size_name = None
        # TODO: Logic to get size saved by user in profile.
        if len(request.current_user.profile.product_types) > 0:
            product_type_name = request.current_user.profile.product_types[0]

        if product_type_name is None:
            response = ProductTypePersonalize.get_recommends(
                customer_id=request.current_user.email, size=1)
            product_type_name = response[0]['name']

        if product_size_name is None:
            candidates = product_model.get_sizes_by_product_type(product_type_name, request.gender)
            if len(candidates) == 0:
                return {
                    'product_type': product_type_name,
                    'product_size': None,
                    'products': []
                }
            else:
                product_size_name = ProductSizePersonalize.get_top_size(candidates, customer_id=request.current_user.email, size=1)

        products = product_model.get_by_size(
            product_size_name, product_type=product_type_name, gender=request.gender,
            page=request.page, size=request.size)

        return {
            'product_type': product_type_name,
            'product_size': product_size_name,
            'products': products
        }

    @blue_print.route('/sizes/metrics')
    def size_metrics(cors=True):
        return ProductSizePersonalize.get_metrics()
