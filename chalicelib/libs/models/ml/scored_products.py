import json
import math
from datetime import datetime, timedelta
from typing import List, Union, Optional, Tuple
from warnings import warn
from elasticsearch import helpers
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.Product import Product, ProductSearchCriteria
from chalicelib.libs.core.datetime import get_mpc_datetime_now, DATETIME_FORMAT
from chalicelib.libs.models.mpc.categories import Category, CategoryEntry
from chalicelib.libs.models.mpc.product_types import ProductType
from chalicelib.libs.models.mpc.product_visit_logs import ProductVisitLog
from chalicelib.libs.models.mpc.brands import Brand
from ..mpc.ProductMapping import scored_products_mapping
from ..mpc.Cms.user_states import CustomerStateModel
from ..mpc.product_tracking import (
    ProductsTrackingModel, _BaseAction, ViewAction, VisitAction, ClickAction)
from ..mpc.Cms.weight import ScoringWeight, WeightModel
from .orders import Order, OrderAggregation
from .utils import get_bucket_data, get_username_from_email
from .tracks import UserTrackEntry
from .weights import ScoringWeight
from .product_entry import ProductEntry, PercentageScoreRange


class ScoredProduct(object):
    INDEX_NAME = settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS
    __TRACK_IN_DATALAKE__: bool = True

    __weight__: ScoringWeight = None

    def __init__(self):
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS,
            settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS
        )

    @property
    def now(self) -> datetime:
        return get_mpc_datetime_now()

    @staticmethod
    def __convert_filter(filter_name):
        switcher = {
            'id': 'portal_config_id',
            'sku': 'rs_sku',
            'title': 'product_name',
            'subtitle': 'product_description',
            'price': 'rs_selling_price',
            'product_type': 'product_size_attribute',
            'product_sub_type': 'rs_product_sub_type',
            'gender': 'gender',
            'brand': 'manufacturer',
            'size': 'sizes.size',
            'color': 'rs_colour',
            'newin': 'created_at',
            # '_score': '_score',
            ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE: 'percentage_score',
            'search_query': 'search_query',
        }
        return switcher.get(filter_name, "invalid_name")

    @staticmethod
    def __convert_sort_filter(column_name, direction):
        sort_map = {
            'id': {'portal_config_id': {'order': direction}},
            'sku': {'rs_sku': {'order': direction}},
            'title': {'product_name': {'order': direction}},
            'subtitle': {'product_description': {'order': direction}},
            'product_type': {'product_size_attribute': {'order': direction}},
            'product_sub_type': {'rs_product_sub_type': {'order': direction}},
            'gender': {'gender': {'order': direction}},
            'brand': {'manufacturer': {'order': direction}},
            'size': {'sizes.size': {'order': direction}},
            'color': {'rs_colour': {'order': direction}},
            'newin': {'created_at': {'order': direction}},
            # '_score': '_score',
            ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE: {'percentage_score': {'order': direction}},
            'search_query': {'search_query': {'order': direction}},
            'price': {
                '_script': {
                    "type": "number",
                    "script": {
                        "lang": "painless",
                        # see __convert_item_calculate_prices()
                        "source": "{price} - {price} * {discount} / 100".format(**{
                            'price': "doc['rs_selling_price'].value",
                            'discount': "doc['discount'].value"
                        }),
                    },
                    "order": direction
                }
            },
        }

        if column_name not in sort_map.keys():
            raise ValueError('Oh, no! {} does not know, how to {} with {} column!'.format(
                ScoredProduct.__qualname__,
                '__convert_sort_filter',
                column_name
            ))

        return sort_map[column_name]

    @property
    def elastic(self) -> Elastic:
        return self.__elastic

    @property
    def weight(self) -> ScoringWeight:
        if not self.__weight__:
            weight_model = WeightModel()
            self.__weight__ = weight_model.scoring_weight
        return self.__weight__

    def __update_by_query(self, query: dict):
        return self.elastic.update_by_query(query)

    def __convert_products(self, data, tier: dict = None, is_anyonimous: bool = False):
        ret ={
            "total": data["total"],
            "products": [self.__convert_item(
                item["_source"], tier=tier,
                is_anyonimous=is_anyonimous) for item in data["hits"]]
        }
        return ret

    @staticmethod
    def __convert_item_calculate_prices(item) -> tuple:
        original_price = float(item['rs_selling_price'] or 0)
        discount = float(item['discount'] or 0)
        current_price = original_price - original_price * discount / 100
        return original_price, current_price

    def __convert_item(self, item, tier: dict = None, is_anyonimous: bool = False):
        original_price, current_price = self.__class__.__convert_item_calculate_prices(item)

        fbucks = None
        if isinstance(tier, dict) and not tier.get('is_neutral') and not is_anyonimous:
            fbucks = math.ceil(item['current_price'] * tier['discount_rate'] / 100)

        result = {
            'id': item['portal_config_id'],
            'sku': item['rs_sku'],
            'event_code': item['event_code'],
            'title': item['product_name'],
            'subtitle': item['product_description'],

            'price': item['rs_selling_price'],
            'discount': item['discount'],
            'original_price': original_price,
            'current_price': current_price,
            'fbucks': fbucks,

            # 'badge': 'NEW IN' if datetime.strptime(item['created_at'], "%Y-%m-%d %H:%M:%S") > from_date else None,
            'product_type': item['product_size_attribute'],
            'product_sub_type': item['rs_product_sub_type'],
            'gender': item['gender'],
            'brand': item['manufacturer'],
            'color': item['rs_colour'],
            'sizes': [{
                'size': size['size'],
                'qty': size['qty'],
                'simple_sku': size['rs_simple_sku'],
                'simple_id': size['portal_simple_id'],
            } for size in item.get('sizes', [])],
            'image': {
                'src': item['images'][0]['s3_filepath'] if len(item['images']) > 0 else 'https://www.supplyforce.com/ASSETS/WEB_THEMES//ECOMMERCE_STD_TEMPLATE_V2/images/NoImage.png',
                'title': item['product_size_attribute'],
            },
            'scores': {
                'version': self.weight.version,
                'qs': item.get('question_score', 0),
                'qw': self.weight.question,
                'rs': item.get('order_score', 0),
                'rw': self.weight.order,
                'ts': item.get('tracking_score', 0),
                'tw': self.weight.track,
                'total': sum([
                    float(item.get('question_score', 0) or 0) * self.weight.question,
                    float(item.get('order_score', 0) or 0) * self.weight.order,
                    float(item.get('tracking_score', 0) or 0) * self.weight.track,
                ]),
                ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE: item.get(ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE, -1.00),
            }
        }

        if not is_anyonimous:
            result.update({
                'tracking_info': item.get('tracking_info', {
                        'views': 0,
                        'clicks': 0,
                        'visits': 0
                    }),
                'is_seen': item.get('is_seen', False)
            })

        return result

    def __bulk(self, actions: List[dict]) -> bool:
        try:
            count, _ = helpers.bulk(self.elastic.client, actions)
            return count > 0
        except Exception as e:
            warn(str(e))
            return False

    def __bulk_update(self, customer_id: str, products: List[ProductEntry]):
        if not customer_id:
            customer_id = 'BLANK'
        # TODO: Index name should not contain the char - "#". Should be updated here.
        actions = [{
                '_index': self.INDEX_NAME,
                '_type': self.INDEX_NAME,
                '_id': "%s__%s" % (customer_id, product.rs_sku),
                '_source': {
                    'customer_id': customer_id,
                    **product.to_dict(mode='scored')
                }
            } for product in products]
        return self.__bulk(actions)

    def __get_tracking_aggregation(
            self, customer_id: str, size: int = 500) -> Tuple[dict, dict]:
        query = {
            "aggs": {
                "product_types": {
                    "terms": {
                        "field": "product_size_attribute",
                        "size": 1000
                    }
                },
                "product_sub_types": {
                    "terms": {
                        "field": "rs_product_sub_type",
                        "size": 1000
                    }
                },
                "genders": {
                    "terms": {
                        "field": "gender",
                        "size": 10
                    }
                },
                "brands": {
                    "terms": {
                        "field": "manufacturer",
                        "size": 1000
                    }
                },
                "sizes": {
                    "terms": {
                        "field": "sizes.size",
                        "size": 1000
                    }
                }
            }, 
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "customer_id": customer_id
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "range": {
                                            "tracking_info.clicks": {
                                                "gt": 0
                                            }
                                        }
                                    },
                                    {
                                        "range": {
                                            "tracking_info.visits": {
                                                "gt": 0
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "size": size
        }
        response = self.elastic.post_search(query)
        KEYS = ['brands', 'sizes', 'product_types', 'genders', 'product_sub_types']
        data = dict()
        for key, agg_data in response['aggregations'].items():
            if key not in KEYS:
                continue
            data[key] = [bucket['key'] for bucket in agg_data['buckets']]
        products = dict()
        for hit in response['hits']['hits']:
            item = hit['_source']
            products[item['rs_sku']] = {
                'views': item.get('tracking_info', {}).get('views', 0),
                'clicks': item.get('tracking_info', {}).get('clicks', 0),
                'visits': item.get('tracking_info', {}).get('visits', 0),
                'viewed_at': item.get('viewed_at'),
            }
        return data, products

    def __build_track_query(self, action_or_list: List[_BaseAction]) -> List[dict]:
        action_maps = {
            ViewAction: 'tracking_info.views',
            ClickAction: 'tracking_info.clicks',
            VisitAction: 'tracking_info.visits'
        }
        buffer = dict()

        if isinstance(action_or_list, _BaseAction):
            action_or_list = [action_or_list]

        # Grouping by action_type and customer_id
        for action in action_or_list:
            if not action_maps.get(action.__class__):
                warn("Unknown instance found - %s" % action.__class__)
                continue

            if buffer.get(action_maps[action.__class__]) is None:
                buffer[action_maps[action.__class__]] = {action.user_id: []}
            
            if buffer[action_maps[action.__class__]].get(action.user_id) is None:
                buffer[action_maps[action.__class__]][action.user_id] = []

            buffer[action_maps[action.__class__]][action.user_id].append(action.config_sku)
        
        queries = list()
        date_str = self.now.strftime("%Y-%m-%d %H:%M:%S")
        for action_type, user_data in buffer.items():
            for customer_id, config_skus in user_data.items():
                if not customer_id:
                    continue

                query = {
                    "script": {
                        "inline": "ctx._source.%s += params.step;"\
                            "ctx._source.viewed_at = params.viewed_at" % action_type,
                        "lang": "painless",
                        "params": {
                            "step": 1,
                            "viewed_at": date_str,
                        },
                        "upsert": {
                            action_type : 1,
                            "viewed_at": date_str,
                        }
                    },
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "customer_id": customer_id
                                    }
                                },
                                {
                                    "terms": {
                                        "rs_sku": config_skus
                                    }
                                }
                            ]
                        }
                    }
                }
                queries.append(query)
        results = list()
        for query in queries:
            results.append(self.__update_by_query(query))

        return results

    def __makeESFilterFromCustomFilter(
            self,
            custom_filters: Optional[dict] = None, customer_id: str = None):
        if not customer_id:
            customer_id = 'BLANK'
        ret = {}
        ret['bool'] = {}
        ret['bool']['must'] = [
            {
                "match": {
                    "customer_id": customer_id
                }
            }
        ]
        for key, value in custom_filters.items() if custom_filters else []:
            key = self.__class__.__convert_filter(key)
            if(key == "invalid_name"):
                continue
            must_item = {}
            if key == 'rs_selling_price':
                must_item['range'] = {}
                must_item['range'][key] = {}
                must_item['range'][key]['gte'] = value[0]
                must_item['range'][key]['lte'] = value[1]
            elif key == 'search_query':
                value = str(value or '').strip()
                if value:
                    must_item['bool'] = {
                        "should": [
                            {"match_phrase_prefix": {"product_name": value}},
                            # {"match_phrase_prefix": {"product_description": value}} When search_query is 'Dress', this returns some socks and shoes
                            {"match_phrase_prefix": {"product_size_attribute": value}}
                        ]
                    }
            elif key == 'created_at':
                if value == 'true':
                    from_date = (
                        self.now - timedelta(
                            days=settings.NEW_PRODUCT_THRESHOLD)
                        ).strftime(DATETIME_FORMAT)
                    must_item['range'] = {}
                    must_item['range'][key] = {}
                    must_item['range'][key]['gte'] = from_date
                else:
                    continue
            else:
                if isinstance(value, list):
                    must_item['bool'] = {}
                    must_item['bool']['should']=[]
                    for _value in value:
                        match={}
                        match['match']={}
                        match['match'][key]=_value
                        must_item['bool']['should'].append(match)
                else:
                    must_item['match']={}
                    must_item['match'][key]=value

            if must_item:
                ret['bool']['must'].append(must_item)

        return ret

    def __get_sort_option_by_score(self) -> dict:
        return {
            "_script": {
                "type": "number",
                "script": {
                    "lang": "painless",
                    "source": "doc['question_score'].value * params.qw +"\
                        "doc['order_score'].value * params.rw +"\
                        "doc['tracking_score'].value * params.tw",
                    "params": {
                        "qw": self.weight.question,
                        "rw": self.weight.order,
                        "tw": self.weight.track,
                    }
                },
                "order": "desc"
            }
        }

    def __get_sort_option_by_viewed_at(self) -> dict:
        return {
            "viewed_at": {
                "order": "asc"
            }
        }

    def __get_inline_script(
            self,
            attr: str,
            value: Union[str, int, float, dict],
            params_name: str = 'params',
            prefix: str = None,
            context_prefix: str = "ctx._source"):
        results = list()
        if prefix:
            attr_name = "%s.%s" % (prefix, attr)
        else:
            attr_name = attr
        if isinstance(value, dict):
            for key, data in value.items():
                results += self.__get_inline_script(key, data, prefix=attr_name)
        else:
            return ["%s.%s = params.%s" % (context_prefix, attr_name, attr_name)]
        return results

    def __get_query_params(
            self,
            attr: str,
            value: Union[str, int, float, dict],
            prefix: str = None):
        results = list()
        if prefix:
            attr_name = "%s.%s" % (prefix, attr)
        else:
            attr_name = attr
        if isinstance(value, dict):
            for key, data in value.items():
                results += self.__get_query_params(key, data, prefix=attr_name)
        else:
            return [(attr_name, value)]
        return results

    def __get_from_index(self, page: int = 1, size: int = 20) -> int:
        fromindex = (int(page) - 1) * int(size)
        if fromindex < 0:
            fromindex = 0
        return fromindex

    def calculate_scores(
            self, email: str = None, size: int = 500):
        username: str = None
        if email:
            username: str = get_username_from_email(email)

        if username:
            # Track personalize progress
            customer_state = CustomerStateModel(username, email)
            customer_state.personalize_in_progress = True
        username, products = get_bucket_data(
            email, username=username, size=size)
        if username:
            trackings, tracking_dictionary = self.__get_tracking_aggregation(
                username, size=size)
            tracking_data = UserTrackEntry(len(products), **trackings)
            for product in products:
                if tracking_dictionary.get(product.rs_sku):
                    product.views = tracking_dictionary[product.rs_sku]['views']
                    product.clicks = tracking_dictionary[product.rs_sku]['clicks']
                    product.visits = tracking_dictionary[product.rs_sku]['visits']
                    product.viewed_at = tracking_dictionary[product.rs_sku]['viewed_at']
                product.apply_trackings(tracking_data)

        # NOTE: Calculating percentage score
        score_range = PercentageScoreRange()
        for product in products:
            product.score_range = score_range
            if product.total_score > score_range.max_score:
                score_range.max_score = product.total_score
            if product.total_score < score_range.min_score:
                score_range.min_score = product.total_score

        response = self.__bulk_update(username, products)
        if username:
            customer_state.personalize_in_progress = False
        return response

    def track(self, action_or_list: Union[_BaseAction, List[_BaseAction]]):
        self.__update_by_query(self.__build_track_query(action_or_list))

        if isinstance(action_or_list, _BaseAction):
            action_or_list = [action_or_list]

        customer_ids = list(set([
            item.user_id for item in action_or_list
            if isinstance(item, (ClickAction, VisitAction))]))
        for customer_id in customer_ids:
            CustomerStateModel(customer_id).clicked_now()

        # Keep the original tracking module for now.
        if self.__TRACK_IN_DATALAKE__:
            ProductsTrackingModel.track(action_or_list)

    def listByCustomFilter(
        self,
        customer_id: str = None,
        email: str = None,
        custom_filters: Optional[dict] = None,
        sorts: dict = {},
        sort_by_score: bool = True,
        tier: dict = None,
        page=1,
        size=18
    ):
        if not customer_id and isinstance(email, str):
            customer_id = get_username_from_email(email)

        filters = self.__makeESFilterFromCustomFilter(custom_filters, customer_id=customer_id)

        # NOTE: Always score by percentage score
        percentage_score_column = ProductSearchCriteria.SORT_COLUMN_PERCENTAGE_SCORE
        sorts[percentage_score_column] = sorts.get(percentage_score_column) or "desc"

        query = {
            "query": filters,
            "size": size,
            "from": self.__get_from_index(page=page, size=size),
            "sort": [
                self.__class__.__convert_sort_filter(column, direction)
                for column, direction in sorts.items()
            ],
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response, tier=tier, is_anyonimous=(not customer_id))

    def update(self, config_sku: str, data: dict):
        json_data = {
            "doc": data
        }
        inline_scripts = list()
        params_list = list()
        
        for key, value in data.items():
            inline_scripts.append("ctx._source.%s = params.%s" % (key, key))

        query = {
            "script": {
                "inline": ";".join(inline_scripts),
                "lang": "painless",
                "params": data,
                "upsert": data
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "rs_sku": config_sku
                            }
                        }
                    ]
                }
            }
        }
        response = self.__update_by_query(query)
        return response

    def get_new_products(
            self,
            customer_id: str = None, gender: str = None, tier: dict = None,
            page: int = 1, size: int = 20, **kwargs):
        filters = {
                'gender': [gender] if gender and gender.strip().lower() != 'unisex' else [],
                'newin': 'true'
            }
        filters = self.__makeESFilterFromCustomFilter(
            filters, customer_id=customer_id)
        sort_options = [self.__get_sort_option_by_score()]

        query = {
            "query": filters,
            "size": size,
            "from": self.__get_from_index(page=page, size=size),
            "sort": sort_options,
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response, tier=tier, is_anyonimous=(not customer_id))

    def get_last_chance(
            self, customer_id: str = None, gender: str = None, tier: dict = None,
            page=1, size=20, **kwargs):
        end_date = (self.now - timedelta(
                days=settings.LAST_CHANCE_END_DATE_THRESHOLD)
            ).strftime(DATETIME_FORMAT)
        if not customer_id:
            customer_id = 'BLANK'
        offset = self.__get_from_index(page=page, size=size)
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "customer_id": customer_id
                            }
                        },
                        {
                            "range": {
                                "created_at": {"lt": end_date}
                            }
                        },
                        {
                            "range": {
                                "sizes.qty": {
                                    "lte": settings.LAST_CHANCE_STOCK_THRESHOLD,
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "sum_of_order_score": {
                    "sum": {
                        "field": "order_score"
                    }
                },
                "sum_of_question_score": {
                    "sum": {
                        "field": "question_score"
                    }
                },
                "sum_of_tracking_score": {
                    "sum": {
                        "field": "tracking_score"
                    }
                },
                "sum_sort": {
                    "bucket_sort": {
                        "sort": [
                            {
                                "sum_of_question_score": {"order": "desc"}
                            },
                            {
                                "sum_of_order_score": {"order": "desc"}
                            },
                            {
                                "sum_of_tracking_score": {"order": "desc"}
                            }
                        ]
                    }
                }
            },
            "size": 0
        }

        if gender and gender.strip().lower() != 'unisex':
            query['query']['bool']['must'].append(
                {
                    "terms": {
                        "gender": [gender]
                    }
                },
            )

        response = self.__elastic.post_search(query)
        bucket = response['aggregations']['product_type_terms']['buckets'][offset:offset + size]

        product_type_model = ProductType()
        product_types = product_type_model.filter_by_product_type_name([item['key'] for item in bucket])
        dictionary = dict([(item['key'], item['doc_count']) for item in bucket])
        return [{
            'id': int(item['product_type_id']),
            'name': item['product_type_name'],
            'count': dictionary.get(item['product_type_name']),
            'image': {
                'src': item['image'], 'title': item['product_type_name']
            }
        } for item in product_types]

    def get(
            self,
            id,  # config_sku
            customer_id: str = None,
            tier: dict = None,
            log: bool = False,
            session_id: str = None):
        if not customer_id:
            customer_id = 'BLANK'
        item = self.elastic.get_data(f"{customer_id}__{id}")
        if not item:
            response = self.elastic.post_search({
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"customer_id": customer_id}},
                            {"term": {"rs_sku": id}}
                        ]
                    }
                },
                "size": 1
            })['hits']
            if response['total'] > 0:
                item = response['hits'][0]['_source']

        if log and isinstance(item, dict):
            # TODO: refactoring - move out from model's method
            log_model = ProductVisitLog(session_id, customer_id=customer_id)
            log_model.insert(self.__convert_item(item, tier=tier))

        # Fix "An AttributeValue may not contain an empty string" error
        item['size_chart'] = item.get('size_chart') or None
        item['img'] = {
            'media_gallery': item.get('img', {}).get('media_gallery', []),
            'images': {
                'lifestyle': item.get('img', {}).get('images', {}).get('lifestyle') or None,
                'small': item.get('img', {}).get('images', {}).get('small') or None,
                'back': item.get('img', {}).get('images', {}).get('back') or None,
            }
        }

        return item

    def get_categories_by_gender(
            self, gender: str, customer_id: str = None,
            user_defined_product_types: list = [], **kwargs):
        if not customer_id:
            customer_id = 'BLANK'
        if not gender or gender.lower() == 'unisex':
            gender = 'ladies'

        categories = Category().get_by_gender(gender)
        product_types = [item['product_type_name'] for item in categories]

        # NOTE: Filter use defined products by stored categories
        user_defined_product_types = [
            item for item in user_defined_product_types
            if item in product_types]

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "customer_id": customer_id
                            }
                        },
                        {
                            "term": {
                                "gender": gender.upper()
                            }
                        },
                        {
                            "terms": {
                                "product_size_attribute": product_types
                            }
                        },
                        {
                            "range": {
                                "sizes.qty": {
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "product_type_terms": {
                    "terms": {
                        "field": "product_size_attribute"
                    },
                    "aggs": {
                        "sum_of_order_score": {
                            "sum": {
                                "field": "order_score"
                            }
                        },
                        "sum_of_question_score": {
                            "sum": {
                                "field": "question_score"
                            }
                        },
                        "sum_of_tracking_score": {
                            "sum": {
                                "field": "tracking_score"
                            }
                        },
                        "sum_sort": {
                            "bucket_sort": {
                                "sort": [
                                    {
                                        "sum_of_question_score": {"order": "desc"}
                                    },
                                    {
                                        "sum_of_order_score": {"order": "desc"}
                                    },
                                    {
                                        "sum_of_tracking_score": {"order": "desc"}
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            "size": 0
        }

        response = self.__elastic.post_search(query)
        buckets = response['aggregations']['product_type_terms']['buckets']

        sorted_product_types = [item['key'] for item in buckets]
        # NOTE: Re-sort whether it liked by customer or not
        sorted_product_types = user_defined_product_types +\
            [item for item in sorted_product_types if item not in user_defined_product_types]

        return sorted(
            categories,
            key=lambda x: sorted_product_types.index(
                x['product_type_name']) if x['product_type_name'] in sorted_product_types
                else len(categories))

    def get_complete_looks(
            self, id, customer_id='BLANK', tier: dict=None,
            page=1, size=20, **kwargs):
        offset = (page - 1) * size
        item = self.get(id, customer_id=customer_id, tier=tier)
        if item is None:
            return []
        product_type = item.get('product_size_attribute')
        sub_type = item.get('rs_product_sub_type')
        gender = item.get('gender')
        # product_type_model = ProductType()
        # item = product_type_model.get_root_node(product_type_name=product_type)

        product_types = [
            item['product_type_name'] for item in self.get_categories_by_gender(
                gender, customer_id=customer_id, size=5)]

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {"gender": gender}
                        },
                        {
                            "range": {"sizes.qty": {"gt": 0}}
                        },
                        # {
                        #     "terms": {"product_size_attribute": product_types}
                        # },
                        {
                            "term": {"product_size_attribute": product_type}
                        },
                    ],
                    "must_not": [
                        {
                            "term": {"rs_product_sub_type": sub_type}
                        }
                    ],
                }
            },
            "from": offset,
            "size": size
        }
        response = self.elastic.post_search(query)

        return self.__convert_products(response['hits'], tier=tier)['products']


    def get_sizes_by_product_type(
            self, product_type: str, gender: str,
            customer_id: str = 'BLANK', **kwargs):
        if not customer_id:
            customer_id = 'BLANK'
        query = {
            "bool": {
                "must": [
                    {"term": {"product_size_attribute":product_type}},
                    {'term': {'customer_id': customer_id}},
                    {"range": {"sizes.qty": {"gt": 0}}}
                ]
            }
        }
        if gender.lower() != 'unisex':
            query['bool']['must'].append({"term": {"gender": gender}})

        aggs = {
            "product_size_terms": {
                "terms": {
                    "field": "sizes.size"
                },
                "aggs": {
                    "sum_of_order_score": {
                        "sum": {
                            "field": "order_score"
                        }
                    },
                    "sum_of_question_score": {
                        "sum": {
                            "field": "question_score"
                        }
                    },
                    "sum_of_tracking_score": {
                        "sum": {
                            "field": "tracking_score"
                        }
                    },
                    "sum_sort": {
                        "bucket_sort": {
                            "sort": [
                                {
                                    "sum_of_question_score": {"order": "desc"}
                                },
                                {
                                    "sum_of_order_score": {"order": "desc"}
                                },
                                {
                                    "sum_of_tracking_score": {"order": "desc"}
                                }
                            ]
                        }
                    }
                }
            }
        }
        response = self.elastic.post_search({
            'query': query,
            'aggs': aggs,
            'size': 0
        })
        return [item['key'] for item in response['aggregations']['product_size_terms']['buckets']]


    def get_by_size(
            self, product_size: str,
            customer_id: str = None,
            product_type: str = None,
            gender: str = None,
            tier: dict = None,
            page: int = 1, size: int = 20, **kwargs):
        offset = (page - 1) * size
        if not customer_id:
            customer_id = 'BLANK'
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"customer_id": customer_id}},
                        {"term": {"sizes.size": product_size}},
                        {"range": {"sizes.qty": {"gt": 0}}}
                    ]
                }
            },
            "from": offset,
            "size": size
        }

        if product_type:
            query['query']['bool']['must'].append({
                "term": {"product_size_attribute": product_type}
            })
        
        if gender and gender.lower() != 'unisex':
            query['query']['bool']['must'].append({
                'term': {'gender': gender}
            })

        response = self.elastic.post_search(query)

        return self.__convert_products(response['hits'], tier=tier)['products']

    def get_top_brands(
            self, customer_id: str = 'BLANK',
            user_defined: List[str] = [], exclude: List[str] = [],
            page: int = 1, size: int = 20, **kwargs) -> List[dict]:
        offset = (page - 1) * size
        exclude = [item.strip().lower() for item in exclude]
        from_date = (
            self.now - timedelta(
                days=settings.NEW_PRODUCT_THRESHOLD)
            ).strftime(DATETIME_FORMAT)

        if not customer_id:
            customer_id = 'BLANK'

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"customer_id": customer_id}},
                        {"range": {"sizes.qty": {"gt": 0}}},
                    ],
                    "must_not": [
                        {"terms": {"brand_code": exclude}}
                    ]
                }
            },
            "aggs": {
                "available_brands": {
                    "terms": {"field": "manufacturer", "size": 1000},
                    "aggs": {
                        "new_items": {
                            "range": {
                                "field": "created_at",
                                "ranges": [{"from": from_date}]
                            }
                        },
                        "sum_of_order_score": {
                            "sum": {
                                "field": "order_score"
                            }
                        },
                        "sum_of_question_score": {
                            "sum": {
                                "field": "question_score"
                            }
                        },
                        "sum_of_tracking_score": {
                            "sum": {
                                "field": "tracking_score"
                            }
                        },
                        "sum_sort": {
                            "bucket_sort": {
                                "sort": [
                                    {
                                        "sum_of_question_score": {"order": "desc"}
                                    },
                                    {
                                        "sum_of_order_score": {"order": "desc"}
                                    },
                                    {
                                        "sum_of_tracking_score": {"order": "desc"}
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            "size": 0
        }

        response = self.elastic.post_search(query)

        brand_model = Brand()
        buckets = response['aggregations']['available_brands']['buckets']
        brand_names = [item['key'] for item in buckets]
        brand_names = sorted(
            brand_names, key=lambda x: user_defined.index(x)
            if x in user_defined else len(brand_names)
        )[offset: offset + size]
        buckets = dict([(item['key'].lower(), {
                'new': item['new_items']['buckets'][0]['doc_count'] > 0,
                'available_items': item['doc_count'],
                'new_items': item['new_items']['buckets'][0]['doc_count']
            }) for item in buckets if item['key'] in brand_names])
        brands = brand_model.filter_by_brand_names(brand_names)["Items"]

        for brand in brands:
            if buckets.get(brand['brand_name'].lower()):
                brand.update(buckets.get(brand['brand_name'].lower()))
            else:
                del brand
        return brands
