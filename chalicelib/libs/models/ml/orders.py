import boto3
from typing import List, Union
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch_dsl import Search, A
from ....settings import settings
from .demo_orders import OrderAggregation


class Order(object):
    ES_HOST = settings.AWS_ELASTICSEARCH_HOST
    ES_PORT = settings.AWS_ELASTICSEARCH_PORT
    ES_REGION = settings.AWS_ELASTICSEARCH_PRODUCTS_REGION
    INDEX_NAME = settings.AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS
    DOC_TYPE = settings.AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS

    def __init__(self, **kwargs):
        service = 'es'
        credentials = boto3.Session(region_name=self.ES_REGION).get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, self.ES_REGION, service)

        self.__es = Elasticsearch(
            hosts=[{'host': self.ES_HOST, 'port': self.ES_PORT}],
            # http_auth = awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    @property
    def elasticsearch(self):
        return self.__es

    def convert_item(self, item: dict) -> dict:
        from_date = datetime.now() - timedelta(days=settings.NEW_PRODUCT_THRESHOLD)
        item = {
            'id': item['portal_config_id'],
            'sku': item['rs_sku'],
            'title': item['product_name'],
            'subtitle': item['product_description'],
            'price': Decimal(0 if item['rs_selling_price'] is None else item['rs_selling_price']),
            'badge': 'NEW IN' if datetime.strptime(item['created_at'], "%Y-%m-%d %H:%M:%S") > from_date else None,
            'favorite': random.choice([True, False]),
            'product_type': item['product_size_attribute'],
            'product_sub_type': item['rs_product_sub_type'],
            'gender': item['gender'],
            'brand': item['manufacturer'],
            'sizes': [{
                'size': size['size'],
                'qty': size['qty'],
                'rs_simple_sku': size['rs_simple_sku']
            } for size in item['sizes']],
            'image': {
                'src': item['images'][0]['s3_filepath'] if len(item['images']) > 0 else 'https://placeimg.com/155/140/arch',
                'title': item['product_size_attribute'],
            },
        }

        if tier is not None and type(tier) == dict:
            item['fbucks'] = math.ceil(item['price'] * tier.get('discount_rate') / 100)
        return item

    def convert(
            self, products, personalize=False, customer_id='BLANK',
            **kwargs) -> List[dict]:
        if personalize:
            config_skus = ConfigSkuPersonalize.get_personalized_ranking(
                [item['rs_sku'] for item in products], customer_id=customer_id)
            products = sorted(products, key=lambda x: config_skus.index(x['rs_sku']))

        return [self.convert_item(item) for item in products]

    def get_order_aggregation(
            self,
            email: Union[str, List[str]]=None,
            **kwargs) -> OrderAggregation:

        if isinstance(email, str):
            email = [email]

        if email is None:
            query = {
                "match_all": {}
            }
        else:
            query = {
                "terms": {
                    "email": email
                }
            }

        aggs = {
            "product_types": {
                "terms": {
                    "field": "product_size_attribute",
                    "size": 1000
                }
            },
            "brands": {
                "terms": {
                    "field": "manufacturer",
                    "size": 100000
                }
            },
            "sizes": {
                "terms": {
                    "field": "size",
                    "size": 100000
                }
            },
            "genders": {
                "terms": {
                    "field": "gender",
                    "size": 100000
                }
            },
            "colors": {
                "terms": {
                    "field": "rs_colour",
                    "size": 100000
                }
            },
        }
        
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)
        s = s.update_from_dict({
            "query": query,
            "aggs": aggs,
            "size": 1
        })

        response = s.execute()
        aggregation = OrderAggregation()
        for gender in response.aggregations.genders.buckets:
            aggregation.append_gender(gender['key'])

        for brand in response.aggregations.brands.buckets:
            aggregation.append_brand(brand['key'])

        for product_type in response.aggregations.product_types.buckets:
            aggregation.append_product_type(product_type['key'])

        for size in response.aggregations.sizes.buckets:
            aggregation.append_size(size['key'])

        for color in response.aggregations.colors.buckets:
            aggregation.append_color(color['key'])

        return aggregation
