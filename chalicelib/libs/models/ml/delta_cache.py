import math
import boto3
import random
from typing import List, Union, Optional
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch_dsl import Search, A
from requests_aws4auth import AWS4Auth
from decimal import Decimal
from ....settings import settings
from .questions import Answer
from .orders import OrderAggregation
from .utils import get_bucket_data


class ProductItem(object):
    __rs_sku: str
    __personalize_score: int
    __personalize_weight: float = 1.0
    __questions_score: int
    __questions_weight: float = 1.0
    __orders_score: int
    __orders_weight: float = 1.0
    __tracking_score: int
    __tracking_weight: float = 1.0

    def __init__(
            self,
            rs_sku: str,
            ps: int = 0,
            qs: int = 0,
            os: int = 0,
            ts: int = 0,
            pw: float = 1.0,
            qw: float = 1.0,
            ow: float = 1.0,
            tw: float = 1.0,
            **kwargs):
        self.rs_sku = rs_sku
        self.personalize_score = ps
        self.question_score = qs
        self.order_score = os
        self.tracking_score = ts
        self.personalize_weight = pw
        self.questions_weight = qw
        self.orders_weight = ow
        self.tracking_weight = tw

    @property
    def rs_sku(self) -> str:
        return self.__rs_sku

    @rs_sku.setter
    def rs_sku(self, value: str):
        self.__rs_sku = value.strip()

    @property
    def personalize_score(self) -> int:
        return self.__personalize_score
    
    @personalize_score.setter
    def personalize_score(self, value: int):
        self.__personalize_score = int(value)

    @property
    def personalize_weight(self) -> float:
        return self.__personalize_weight

    @personalize_weight.setter
    def personalize_weight(self, value: Union[str, int, float, Decimal]):
        if isinstance(value, (int, float, Decimal, str)):
            self.__personalize_weight = float(value)

    @property
    def question_score(self) -> int:
        return self.__questions_score
    
    @question_score.setter
    def question_score(self, value: int):
        self.__questions_score = int(value)

    @property
    def questions_weight(self) -> float:
        return self.__questions_weight

    @questions_weight.setter
    def questions_weight(self, value: Union[str, int, float, Decimal]):
        if isinstance(value, (int, float, Decimal, str)):
            self.__questions_weight = float(value)

    @property
    def order_score(self) -> int:
        return self.__orders_score
    
    @order_score.setter
    def order_score(self, value: int):
        self.__orders_score = int(value)

    @property
    def orders_weight(self) -> float:
        return self.__orders_weight

    @orders_weight.setter
    def orders_weight(self, value: Union[str, int, float, Decimal]):
        if isinstance(value, (int, float, Decimal, str)):
            self.__orders_weight = float(value)

    @property
    def tracking_score(self) -> int:
        return self.__tracking_score
    
    @tracking_score.setter
    def tracking_score(self, value: int):
        self.__tracking_score = value

    @property
    def tracking_weight(self) -> float:
        return self.__tracking_weight

    @tracking_weight.setter
    def tracking_weight(self, value: Union[str, int, float, Decimal]):
        if isinstance(value, (int, float, Decimal, str)):
            self.__tracking_weight = float(value)

    @property
    def total_score(self) -> int:
        return sum([
                self.personalize_score * self.personalize_weight,
                self.question_score * self.questions_weight,
                self.order_score * self.orders_weight,
                self.tracking_score * self.tracking_weight,
            ])

    def to_dict(self):
        return {
            "rs_sku": self.rs_sku,
            "ps": self.personalize_score,
            "pw": self.personalize_weight,
            "qs": self.question_score,
            "qw": self.questions_weight,
            "os": self.order_score,
            "ow": self.orders_weight,
            "ts": self.tracking_score,
            "tw": self.tracking_weight,
            "total": self.total_score
        }


class CacheEntry(object):
    __email: str
    __products: List[ProductItem]

    def __init__(
            self,
            email: str,
            products: Union[List[ProductItem], List[dict]], **kwargs):
        self.email = email
        self.products = products

    @property
    def email(self) -> str:
        return self.__email

    @email.setter
    def email(self, value: str):
        self.__email = value.strip()

    @property
    def products(self) -> List[ProductItem]:
        return self.__products

    @products.setter
    def products(self, value: Union[List[ProductItem], List[dict]]):
        if all(isinstance(x, ProductItem) for x in value):
            self.__products = value
        elif all(isinstance(x, dict) for x in value):
            self.__products = [ProductItem(**item) for item in value]
        else:
            self.__products = []

    def to_dict(self) -> dict:
        return {
            'email': self.email,
            'products': [item.to_dict() for item in self.products],
        }


class DeltaCache(object):
    ES_HOST = settings.AWS_ELASTICSEARCH_HOST
    ES_PORT = settings.AWS_ELASTICSEARCH_PORT
    ES_REGION = settings.AWS_ELASTICSEARCH_PRODUCTS_REGION
    INDEX_NAME = settings.AWS_ELASTICSEARCH_DELTA_CACHE
    DOC_TYPE = settings.AWS_ELASTICSEARCH_DELTA_CACHE

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

    def create_index(self, index, body, recreate=False, **kwargs):
        if not settings.DEBUG:
            raise Exception("Not permitted in production mode.")

        es = self.elasticsearch
        if recreate:  # Delete if recreate required.
            try:
                from warnings import warn
                warn("Please double check! This will remove aws elastic index. And comment out")
                # res = es.indices.delete(index)
                print(res)
            except Exception as e:
                print(str(e))
        return es.indices.create(index, body)

    def insert(self, item, **kwargs):
        es = self.elasticsearch
        return es.index(index=self.INDEX_NAME, doc_type=self.DOC_TPYE, id=item.get('email'), body=item)

    def update(self, email: str, count: int = 500):
        username, products = get_bucket_data(email, size=count)
        data = CacheEntry(email, [product.to_dict(mode='cache') for product in products])
        es = self.elasticsearch
        return es.index(
            index=self.INDEX_NAME, doc_type=self.DOC_TYPE,
            id=email, body=data.to_dict())

    def convert_item(self, item: dict, tier: dict=None) -> dict:
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

    def get(self, email: str, **kwargs) -> List[ProductItem]:
        try:
            response = self.elasticsearch.get(self.INDEX_NAME, email, doc_type=self.DOC_TYPE)['_source']
            item = CacheEntry(**response)
            return item.products
        except Exception as e:
            print(str(e))
            return []

    def convert(
            self, products, personalize=False, customer_id='BLANK',
            tier: dict=None, **kwargs) -> List[dict]:
        if personalize:
            config_skus = ConfigSkuPersonalize.get_personalized_ranking(
                [item['rs_sku'] for item in products], customer_id=customer_id)
            products = sorted(products, key=lambda x: config_skus.index(x['rs_sku']))

        return [self.convert_item(item, tier=tier) for item in products]
