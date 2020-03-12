import boto3
from typing import List, Union
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch_dsl import Search, A
from ....settings import settings


class OrderAggregation:
    genders: List[str] = []
    colors: List[str] = []
    sizes: List[str] = []
    product_types: List[str] = []
    brands: List[str] = []
    product_count: int = 500

    def __init__(self, product_count: int = 500):
        self.product_count = product_count

    def append_gender(self, gender: str):
        if gender.lower() not in self.genders:
            self.genders.append(gender.lower())

    def append_color(self, color: str):
        if type(color) is str:
            color = color.lower()
        if color not in self.colors:
            self.colors.append(color)

    def append_size(self, size: str):
        if type(size) == str:
            size = size.lower()
        if size not in self.sizes:
            self.sizes.append(size)

    def append_product_type(self, product_type: str):
        if product_type.lower() not in self.product_types:
            self.product_types.append(product_type.lower())

    def append_brand(self, brand: str):
        if brand.lower() not in self.brands:
            self.brands.append(brand.lower())

    @property
    def gender_score(self):
        return self.product_count / max(1, len(self.genders))

    @property
    def color_score(self):
        return self.product_count / max(1, len(self.colors))

    @property
    def size_score(self):
        return self.product_count / max(1, len(self.sizes))

    @property
    def brand_score(self):
        return self.product_count / max(1, len(self.brands))

    @property
    def product_type_score(self):
        return self.product_count / max(1, len(self.product_types))

    @property
    def score_factors(self) -> dict:
        return {
            "gender": {
                "values": self.genders,
                "score": self.gender_score},
            "rs_color": {
                "values": self.colors,
                "score": self.color_score},
            "product_size_attribute": {
                "values": self.product_types,
                "score": self.product_type_score},
            "sizes.size.size": {
                "values": self.sizes,
                "score": self.size_score},
            "manufacturer": {
                "values": self.brands,
                "score": self.brand_score},
        }


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
