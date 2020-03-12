import math
import boto3
import random
from typing import List
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch_dsl import Search, A
from requests_aws4auth import AWS4Auth
from decimal import Decimal
from ..mpc.product_types import ProductType
from ..mpc.product_visit_logs import ProductVisitLog
from ....settings import settings
from .orders import OrderAggregation
from ..mpc.Cms.UserQuestions import UserQuestionEntity as Question
from .product_entry import ProductEntry



class Product(object):
    ES_HOST = settings.AWS_ELASTICSEARCH_HOST
    ES_PORT = settings.AWS_ELASTICSEARCH_PORT
    ES_REGION = settings.AWS_ELASTICSEARCH_PRODUCTS_REGION
    INDEX_NAME = settings.AWS_ELASTICSEARCH_PRODUCTS
    DOC_TYPE = settings.AWS_ELASTICSEARCH_PRODUCTS

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
                warn("Are you sure that you want to remove this index?\n"\
                    "Double Check and check the next line and enable!")
                # res = es.indices.delete(index)
                # print(res)
            except Exception as e:
                print(str(e))
        return es.indices.create(index, body)

    def bulk_insert(self, index, doc_type, items, recreate=False, random_date=False, **kwargs):
        es = self.elasticsearch
        if recreate:  # Delete if recreate required.
            res = es.indices.delete(index)
        actions = []
        for item in items:
            if random_date:
                MAX_DAY = 10
                new_datetime = (datetime.now() - timedelta(days=random.randint(0, MAX_DAY))).strftime("%Y-%m-%d %H:%M:%S")
                item.update({
                    "created_at": new_datetime,
                    "updated_at": new_datetime
                })
            item.update({
                'brand_code': item.get('manufacturer', '').lower()
            })
            actions.append({
                '_index': index,
                '_type': doc_type,
                '_id': item['rs_sku'],
                '_source': item
            })
        return helpers.bulk(es, actions)

    def insert(self, index, doc_type, item, **kwargs):
        es = self.elasticsearch
        item.update({
            'brand_code': item.get('manufacturer', '').lower()
        })
        return helpers.create(es, index, item.get('rs_sku'), item, doc_type)

    def convert_item(self, item, tier: dict=None) -> dict:
        from_date = datetime.now() - timedelta(days=settings.NEW_PRODUCT_THRESHOLD)

        # @todo : refactoring
        original_price = float(item['rs_selling_price']) if item['rs_selling_price'] else 0
        discount = float(item['discount']) if item['discount'] else 0
        current_price = original_price - original_price * discount / 100

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
            'original_price': original_price,
            'current_price': current_price,
        }

        if tier is not None and type(tier) == dict:
            item['fbucks'] = math.ceil(float(item['price']) * float(tier.get('discount_rate')) / 100)
        return item

    def get(self, id, **kwargs):
        return self.elasticsearch.get(self.INDEX_NAME, id, doc_type=self.DOC_TYPE)['_source']

    def convert(
            self, products, personalize=False, customer_id='BLANK',
            tier: dict=None, **kwargs) -> List[dict]:
        return [self.convert_item(item, tier=tier) for item in products]

    def get_bestsellers(self, product_type=None, gender=None, page=1, size=20, **kwargs):
        offset = (page - 1) * size
        end_date = (datetime.now() - timedelta(
            days=settings.LAST_CHANCE_END_DATE_THRESHOLD)).strftime("%Y-%m-%d %H:%M:%S")

        if product_type is None:
            query = {
                "bool": {
                    "should": [
                        {"range": {"created_at": {"lt": end_date}}},
                        {"range": {"sizes.qty": {"lte": settings.LAST_CHANCE_STOCK_THRESHOLD, "gt": 0}}}
                    ]
                }
            }
        else:
            query = {
                "bool": {
                    "should": [
                        {"range": {"created_at": {"lt": end_date}}},
                        {"range": {"sizes.qty": {"lte": settings.LAST_CHANCE_STOCK_THRESHOLD, "gt": 0}}}
                    ],
                    "must": [
                        {"term": {"product_size_attribute": product_type}}
                    ]
                }
            }
        
        if gender is not None and gender.lower() != 'unisex':
            if query['bool'].get('must') is None:
                query['bool']['must'] = list()
            query['bool']['must'].appen({
                "term": {"gender": gender}
            })

        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)\
            .query(query)[offset:offset + size]
        response = s.execute()
        return self.convert([item['_source'] for item in response.hits.hits])

    def get_by_price(self, max_price, min_price=0, page=1, size=20, **kwargs):
        offset = (page - 1) * size
        query = {
            "bool": {
                "must": [
                    {"range": {"rs_selling_price": {
                        "gt": min_price,
                        "lte": max_price
                    }}}
                ]
            }
        }
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)\
            .query(query)[offset:offset + size]
        response = s.execute()

        return self.convert([item['_source'] for item in response.hits.hits], personalize=True)

    def find_by_id(self, id, log=False, session_id=None, user_id=None, tier=None, **kwargs):
        es = self.elasticsearch
        item = es.get(index=self.INDEX_NAME, doc_type=self.DOC_TYPE, id=id, ignore=[404]).get('_source')
        if log and item is not None:
            # TODO: Track logs
            log_model = ProductVisitLog(session_id, customer_id=user_id)
            log_model.insert(self.convert_item(item))
        return self.convert_item(item, tier=tier)

    def get_smiliar_styles(
            self, id, page=1, size=20, customer_id='BLANK',
            tier=None, **kwargs):
        offset = (page - 1) * size
        item = self.find_by_id(id)
        if item is None:
            return []
        product_type = item.get('product_type')
        sub_type = item.get('product_sub_type')
        gender = item.get('gender')
        brand = item.get('brand')
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)\
            .query({
                "bool": {
                    "must": [
                        {
                            "range": {"sizes.qty": {"gt": 0}}
                        },
                        {
                            "term": {"gender": gender}
                        },
                        {
                            "term": {"product_size_attribute": product_type}
                        },
                        {
                            "term": {"rs_product_sub_type": sub_type}
                        },
                        {
                            "match": {"manufacturer": brand}
                        }
                    ],
                    "must_not": [
                        {
                            "match": {
                                "rs_sku": id
                            }
                        }
                    ]
                }
            }).query({
                "function_score" : {
                "query" : { "match_all": {} },
                "random_score" : {}
            }
            })[offset:offset + size]
        response = s.execute()

        return self.convert(
            [item['_source'] for item in response.hits.hits],
            tier=tier)

    def get_also_availables(self, id, tier=None, **kwargs):
        item = self.find_by_id(id)
        if item is None:
            return []

        base_sku = '_'.join(item.get('sku').split('_')[:-1])
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)\
            .query({
                "bool": {
                    "must": [
                        {
                            "range": {"sizes.qty": {"gt": 0}}
                        },
                        {
                            "prefix": {"rs_sku": {"value": base_sku}}
                        }
                    ],
                    "must_not": [
                        {
                            "match": {
                                "rs_sku": id
                            }
                        }
                    ]
                }
            }).query({
                "function_score" : {
                    "query" : { "match_all": {} },
                    "random_score" : {}
                }
            })
        response = s.execute()
        return self.convert([item['_source'] for item in response.hits.hits], tier=tier)

    def get_recently_viewed(
            self, session_id, customer_id=None,
            product_id=None, tier: dict = None, **kwargs):
        log_model = ProductVisitLog(session_id, customer_id=customer_id)
        logs = log_model.get_logs(omit=product_id)
        
        # for log in logs:
        #     discount = float(log.get('discount'))
        #     if log.get('original_price') is None:
        #         log['original_price'] = float(log.get('rs_selling_price'))

        #     original_price = float(log.get('original_price') or 0)
        #     current_price = original_price - original_price * discount / 100
        #     log['current_price'] = current_price
        #     if tier is not None and type(tier) == dict:
        #         log['fbucks'] = math.ceil(log['current_price'] * tier.get('discount_rate') / 100)

        # To implement discount
        skus: List[str] = [log['sku'] for log in logs]
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)\
            .query({
                "terms": {"rs_sku": skus}
            })
        response = s.execute()
        products = self.convert([item['_source'] for item in response.hits.hits], tier=tier)
        return sorted(
            products, key=lambda x: skus.index(x['sku']))

    def get_brands(
            self,
            keyword: str=None,
            prefix: List[str]=[], exclude: List[str]=[],
            page=1, size=1000, **kwargs) -> List[dict]:

        prefix = [item.lower().strip() for item in prefix]
        exclude = [item.lower().strip() for item in exclude]
        offset = (page - 1) * size
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME)
        s = s.update_from_dict({
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "terms": {
                                "brand_code": [item.lower() for item in exclude if item]
                            }
                        }
                    ], 
                    "must": [
                        {
                            "range": {
                                "sizes.qty": {
                                    "gte": 0
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "prefix": {
                                            "brand_code": item.lower()
                                        }
                                    } for item in prefix if item
                                ]
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "brands": {
                    "terms": {
                        "field": "manufacturer",
                        "order": {
                            "_term": "asc"
                        },
                        "size": 1000
                    }
                }
            },
            "size": 10
        })

        response = s.execute()
        buckets = response.aggregations.brands.buckets
        return [item['key'] for item in buckets]

    def get_products_by_category(self, product_type, sub_types, gender, page=1, size=18):
        """Get products for a specific category
        """
        offset = (page - 1) * size
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME).query(
            {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "product_size_attribute": {
                                    "value": product_type
                                }
                            }
                        },
                        {
                            "term": {
                                "gender": {
                                    "value": gender
                                }
                            }
                        },
                        {
                            "terms": {
                                "rs_product_sub_type": sub_types + ['BLANK']
                            }
                        },
                        {
                            "range": {
                                "sizes.qty" : {
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            })[offset: offset + size]

        response = s.execute()
        return self.convert([item['_source'] for item in response.hits.hits])

    def filter_subtypes_in_stock(self, subtypes, gender=None, **kwargs):
        subtype_names = [item['product_type_name'] for item in subtypes]
        if gender is None or gender.lower() == 'unisex':
            query = {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "rs_product_sub_type": subtype_names
                            }
                        },
                        {
                            "range": {
                                "sizes.qty" : {
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            }
        else:
            query = {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "gender": {
                                    "value": gender.upper()
                                }
                            }
                        },
                        {
                            "terms": {
                                "rs_product_sub_type": subtype_names
                            }
                        },
                        {
                            "range": {
                                "sizes.qty" : {
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            }
        s = Search(using=self.elasticsearch, index=self.INDEX_NAME).query(query)

        s.aggs.bucket('subtypes_in_stock', "terms", field='rs_product_sub_type')
        response = s.execute()
        buckets = response.aggregations.subtypes_in_stock.buckets
        buffer = {}
        for bucket in buckets:
            buffer[bucket['key']] = True

        return [item for item in subtypes if buffer.get(item['product_type_name'])]

    def find_by_simple_sku(self, simple_sku, **kwargs):
        config_sku = simple_sku.split('-')[0]
        item = self.get(config_sku)
        for size in item.get('sizes', []):
            if size['rs_simple_sku'] == simple_sku:
                if int(size['qty']) > 0:
                    item.update(size)
                break
        del item['sizes']
        return item
