import boto3
from datetime import datetime
from ..models.ml.products import Product
from ..models.mpc.product_types import ProductType
from ..models.mpc.product_sizes import ProductSize
from . import personalize as engines


class EventTracker:
    @classmethod
    def extract_meta(cls, item):
        gender = item['gender'].upper()
        product_type_name = item['product_size_attribute']\
            if item['rs_product_sub_type'] == 'BLANK'\
            else item['rs_product_sub_type']

        # Workflow to get product type id with product_type_name and gender
        product_type_model = ProductType()
        product_type = product_type_model.get_root_node(product_type_name=product_type_name)
        product_type_id = product_type['product_type_id']
        product_types = product_type_model.get_child_product_types(
            product_type_id, gender=gender)
        product_types = [item for item in product_types if item['product_type_code'] == product_type_name.lower()]
        if len(product_types) > 0:
            product_type = product_types[0]
            product_type_id = product_type['product_type_id']

        # Workflow to get size id to be used in size event tracking.
        size = item.get('size')
        size_id = None
        if size is not None:
            product_size_model = ProductSize()
            product_sizes = product_size_model.filter_by_product_size_name([size])
            if len(product_sizes) > 0:
                size_id = product_sizes[0]

        return {
            'gender': gender,
            'product_type_id': product_type_id,
            'brand': item['manufacturer'],
            'size_id': size_id
        }

    @classmethod
    def tract_item(
            cls, customer_id, session_id, config_sku=None,
            simple_sku=None, event_type='click', **kwargs):
        product_model = Product()
        if config_sku is not None:
            item = product_model.get(config_sku)
        elif simple_sku is not None:
            item = product_model.find_by_simple_sku(simple_sku)
            config_sku = item['rs_sku']
        else:
            raise Exception('config_sku or simple_sku should be given.')

        data = cls.extract_meta(item)
        # Track config sku event
        engines.ConfigSkuPersonalize.track_event(
            customer_id, session_id, config_sku, event_type=event_type)

        # Track gender event
        engines.GenderPersonalize.track_event(
            customer_id, session_id, data['gender'].upper(), event_type=event_type)

        # TODO: Track brand event
        # TODO: Need to expand brand personalize engine with personalized ranking support.
        engines.ProductBrandPersonalize.track_event(
            customer_id, session_id, data['brand'], event_type=event_type)

        # TODO: Track product_type event
        if data.get('product_type_id') is not None:
            engines.ProductTypePersonalize.track_event(
                customer_id, session_id, str(data['product_type_id']), event_type=event_type)

        # TODO: Track size event
        if data.get('size_id') is not None:
            engines.ProductSizePersonalize.track_event(
                customer_id, session_id, str(data['size_id']), event_type=event_type)

    @classmethod
    def track(
        cls, customer_id, session_id,
        simple_sku=None, config_sku=None, gender=None,
        product_type=None, event_type='click', **kwargs):
        """Class method to track events
        There is some relationship between parameters.
        - simple_sku: contains all of the other parameters. So nothing else is required.
        - conig_sku: contains product_type, gender, color, and so on
        - product_type and gender can be exclusive each other.
        """
        try:
            if simple_sku or config_sku:
                cls.tract_item(
                    customer_id, session_id, config_sku=config_sku,
                    simple_sku=simple_sku, event_type=event_type)
            else:
                if product_type is None and gender is None:
                    raise Exception('product_type or gender is required at least to track event.')

                
                if product_type is not None:
                    response = engines.ProductTypePersonalize.track_event(
                        customer_id, session_id, product_type, event_type=event_type)
                if gender is not None:
                    response = engines.GenderPersonalize.track_event(
                        customer_id, session_id, gender.upper(), event_type=event_type)
            return True
        except Exception as e:
            print(str(e))
            return False
