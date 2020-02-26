import json
from datetime import datetime, timedelta
from typing import List, Union, Optional, Tuple
from warnings import warn
from elasticsearch import helpers
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.Product import Product
from chalicelib.libs.core.datetime import get_mpc_datetime_now
from ..mpc.ProductMapping import scored_products_mapping
from ..mpc.product_tracking import (
    ProductsTrackingModel, _BaseAction, ViewAction, VisitAction, ClickAction)
from ..mpc.Cms.weight import ScoringWeight, WeightModel
from .orders import Order, OrderAggregation
from .utils import get_bucket_data
from .tracks import UserTrackEntry
from .weights import ScoringWeight
from .product_entry import ProductEntry


class ScoredProduct(object):
    INDEX_NAME = settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS
    __KEEP_ORIGINAL_TRACKING__: bool = False

    __weight__: ScoringWeight = None

    def __init__(
            self, keep_original_tracking: bool = False):
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS,
            settings.AWS_ELASTICSEARCH_SCORED_PRODUCTS
        )
        self.__KEEP_ORIGINAL_TRACKING__ = keep_original_tracking

    @property
    def now(self) -> datetime:
        return get_mpc_datetime_now()

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
        actions = [{
            '_index': self.INDEX_NAME,
            '_type': self.INDEX_NAME,
            '_id': "%s#%s" % (customer_id, product.rs_sku),
            '_source': {
                'customer_id': customer_id,
                **product.to_dict(mode='scored')
            }
        } for product in products]

        return self.__bulk(actions)

    def calculate_scores(
            self, email: str = None, size: int = 500):
        username, products = get_bucket_data(
            email, size=size)
        # Getting tracking data for the given customer
        # UPDATED THIS RECENTLY
        if username:
            # model = ProductsTrackingModel()
            # trackings = model.get_visited_products_aggregation_data(username)
            trackings = self.__get_tracking_aggregation(username)
            tracking_data = UserTrackEntry(len(products), **trackings)
            for product in products:
                product.apply_trackings(tracking_data)

        return self.__bulk_update(username, products)

    def __get_tracking_aggregation(self, customer_id: str):
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
            "size": 0
        }
        response = self.elastic.post_search(query)
        KEYS = ['brands', 'sizes', 'product_types', 'genders', 'product_sub_types']
        data = dict()
        for key, agg_data in response['aggregations'].items():
            if key not in KEYS:
                continue
            data[key] = [bucket['key'] for bucket in agg_data['buckets']]
        return data

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

    def track(self, action_or_list: Union[_BaseAction, List[_BaseAction]]):
        self.__update_by_query(self.__build_track_query(action_or_list))

        # Keep the original tracking module for now.
        if self.__KEEP_ORIGINAL_TRACKING__:
            model = ProductsTrackingModel()
            model.track(action_or_list)

    def listAll(self, sort, order, customer_id: str = None, page=1, size=18):
        if not customer_id:
            customer_id = 'BLANK'
        fromindex = (int(page) - 1) * int(size)
        if fromindex < 0:
            fromindex = 0

        sort = self.__class__.__convert_filter(sort)
        if sort == "invalid_name":
            return {"error": "invalid sort field"}
        query = {
            "match": {"customer_id": customer_id},
            "size": size,
            "from": fromindex,
            "sort": [{
                sort: {"order": order}
            }]
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response)

    def __makeESFilterFromCustomFilter(self, custom_filters: Optional[dict], customer_id: str = None):
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
                            {"match_phrase_prefix": {"product_description": value}}
                        ]
                    }
            elif key == 'created_at':
                if value == 'true':
                    from_date = (datetime.now() - timedelta(days=settings.NEW_PRODUCT_THRESHOLD)).strftime("%Y-%m-%d %H:%M:%S")
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
                    "source": "doc['personalize_score'].value * params.pw +"\
                        "doc['question_score'].value * params.qw +"\
                        "doc['order_score'].value * params.rw +"\
                        "doc['tracking_score'].value * params.tw",
                    "params": {
                        "pw": self.weight.personalize,
                        "qw": self.weight.question,
                        "rw": self.weight.order,
                        "tw": self.weight.track,
                    }
                },
                "order": "desc"
            }
        }

    def listByCustomFilter(
            self,
            customer_id: str,
            custom_filters: Optional[dict], sorts,
            sort_by_score: bool = True,
            tier: dict = None,
            page=1, size=18):
        filters = self.__makeESFilterFromCustomFilter(custom_filters, customer_id=customer_id)
        sort_options = [{
                self.__class__.__convert_filter(list(item.keys())[0]): list(item.values())[0]
            } for item in sorts]
        if sort_by_score:
            sort_options += [self.__get_sort_option_by_score()]

        fromindex = (int(page) - 1) * int(size)
        if fromindex < 0:
            fromindex = 0

        query = {
            "query": filters,
            "size": size,
            "from": fromindex,
            "sort": sort_options,
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response, tier=tier, is_anyonimous=(not customer_id))

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

    def update(self, config_sku: str, data: dict):
        json_data = {
            "doc": data
        }
        inline_scripts = list()
        params_list = list()
        
        for key, value in data.items():
            # inline_scripts += self.__get_inline_script(key, value)
            inline_scripts.append("ctx._source.%s = params.%s" % (key, key))
            # params_list += self.__get_query_params(key, value)
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

    def __convert_products(self, data, tier: dict = None, is_anyonimous: bool = False):
        ret ={
            "total": data["total"],
            "products": [self.__convert_item(
                item["_source"], tier=tier,
                is_anyonimous=is_anyonimous) for item in data["hits"]]
        }
        return ret

    def __convert_item(self, item, tier: dict = None, is_anyonimous: bool = False):
        from_date = datetime.now() - timedelta(days=settings.NEW_PRODUCT_THRESHOLD)

        # @todo : refactoring
        original_price = float(item['rs_selling_price'] or 0)
        discount = float(item['discount'] or 0)
        current_price = original_price - original_price * discount / 100

        fbucks = None
        if not tier['is_neutral'] and not is_anyonimous:
            fbucks = math.ceil(product_data['current_price'] * tier['discount_rate'] / 100)

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
                'ps': item.get('personalize_score', 0),
                'pw': self.weight.personalize,
                'qs': item.get('question_score', 0),
                'qw': self.weight.question,
                'rs': item.get('order_score', 0),
                'rw': self.weight.order,
                'ts': item.get('tracking_score', 0),
                'tw': self.weight.track,
                'total': sum([
                    float(item.get('personalize_score', 0) or 0) * self.weight.personalize,
                    float(item.get('question_score', 0) or 0) * self.weight.question,
                    float(item.get('order_score', 0) or 0) * self.weight.order,
                    float(item.get('tracking_score', 0) or 0) * self.weight.track,
                ]),
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
            '_score': '_score',
            'search_query': 'search_query'
        }
        return switcher.get(filter_name, "invalid_name")

    def get(
        self,
        id,
        customer_id: str = None
    ):
        if customer_id:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "rs_sku": id
                                }
                            },
                            {
                                "term": {
                                    "customer_id": customer_id
                                }
                            }
                        ]
                    }
                }
            }
        else:
            query = {
                "query": {
                    "match": {
                        "rs_sku": id
                    }
                }
            }
        response = self.elastic.post_search(query)

        if len(response['hits']['hits']) > 0:
            return response['hits']['hits'][0]['_source']
        else:
            return None

    def updateStock(self, items):
        try:
            total_found_item_count = 0
            updated_item_count = 0
            added_item_count = 0
            for item in items:
                script = "for(int j = 0; j < ctx._source.sizes.size(); j++) "\
                    "if(ctx._source.sizes[j].rs_simple_sku == '%s')"\
                        "{ ctx._source.sizes[j].qty = ; break; }" % (
                        item['rs_simple_sku'], str(item['qty']))
                query = {
                    "script" : script,
                    "query": {
                        "bool": {
                            "must": [
                                { "match": { "sizes.portal_simple_id": item['product_simple_id'] }},
                                { "match": { "sizes.rs_simple_sku": item['rs_simple_sku'] }}
                            ]
                        }
                    }
                }
                res = self.__elastic.update_by_query(query)
                total_found_item_count += res['total']
                updated_item_count += res['updated']
                if res['total'] == 0:
                    sku_size = item['rs_simple_sku'].split('-')
                    rs_sku = sku_size[0]
                    size = sku_size[1]
                    product = self.get(rs_sku)
                    if product is not None:
                        _inline = "ctx._source.sizes.add(params.size)"
                        _size = {
                            "size": size,
                            "portal_simple_id": item['product_simple_id'],
                            "qty": item['qty'],
                            "rs_simple_sku": item['rs_simple_sku'],
                        }
                        _query = {
                            "script": {
                                "lang": "painless",
                                "inline": _inline,
                                "params": {
                                    "size": _size
                                }
                            }
                        }
                        res = self.__elastic.update_data(rs_sku, _query)
                        if res['_id'] == rs_sku:
                            added_item_count += 1

            return {'total_found_item': total_found_item_count, 'updated_item': updated_item_count, 'added_item': added_item_count}
        except:
            return {'result': 'failure'}

    def getRawDataBySimpleSkus(self, simple_skus: Union[Tuple[str], List[str]], convert=True) -> Tuple[dict]:
        response_items = self.__elastic.post_search({
            'query': {
                'bool': {
                    'filter': {
                        'terms': {'sizes.rs_simple_sku': simple_skus}
                    }
                }
            },
            'size': 10000,
        }).get('hits', {}).get('hits', []) or []

        result = [self.__convert_item(data['_source']) if convert else data['_source'] for data in response_items]
        return tuple(result)

    def getRawDataBySimpleSku(self, simple_sku: str, convert=True) -> Optional[dict]:
        rows = self.getRawDataBySimpleSkus([simple_sku], convert)
        return rows[0] if rows else None

    def get_raw_data(self, config_sku: str, convert=False) -> Optional[dict]:
        try:
            product_data = self.__elastic.post_search({
                'query': {
                    'term': {'rs_sku': config_sku}
                }
            }).get('hits', {}).get('hits', [{}])[0].get('_source')
            result = self.__convert_item(product_data) if convert else product_data
            return result
        except:
            return None

    def bulk_insert(self, index, doc_type, items, recreate=False, random_date=False, **kwargs):
        es = self.elastic
        if recreate:  # Delete if recreate required.
            res = es.indices.delete(es.index_name)
        actions = []
        for item in items:
            if random_date:
                MAX_DAY = 10
                new_datetime = (get_mpc_datetime_now() - timedelta(
                    days=random.randint(0, MAX_DAY))).strftime("%Y-%m-%d %H:%M:%S")
                item.update({
                    "created_at": new_datetime,
                    "updated_at": new_datetime
                })
            item.update({
                'customer_id': 'BLANK',
                'brand_code': item.get('manufacturer', '').lower(),
                'tracking_info' : {
                    'visits' : 0,
                    'clicks' : 0,
                    'views' : 0
                },
                'is_seen': False,
            })
            actions.append({
                '_index': es.index_name,
                '_type': es.doc_type,
                '_id': "%s#%s" % ('BLANK', item['rs_sku']),
                '_source': item
            })
        return helpers.bulk(es, actions)
