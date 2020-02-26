import uuid
from typing import Optional, List, Union
from datetime import datetime
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.core.data_lake import DataLakeBase
from chalicelib.libs.core.datetime import get_mpc_datetime_now
from ..mpc.Cms.weight import WeightModel


class ACTION_TYPE:
    view = 'view'
    click = 'click'
    visit = 'visit'


class _BaseAction(object):
    def __init__(
        self,
        raw_product_data: dict,
        session_id: str,
        user_id: Optional[str] = None,
        user_tier: Optional[dict] = None,
        weight_version: int = None,
        personalize_score: float = None,
        personalize_weight: float = None,
        question_score: float = None,
        question_weight: float = None,
        order_score: float = None,
        order_weight: float = None,
        tracking_score: float = None,
        tracking_weight: float = None,
    ):
        if not isinstance(raw_product_data, dict):
            raise ArgumentTypeException(self.__init__, 'raw_product_data', raw_product_data)
        elif not raw_product_data:
            raise ArgumentCannotBeEmptyException(self.__init__, 'raw_product_data')

        if not isinstance(session_id, str):
            raise ArgumentTypeException(self.__init__, 'session_id', session_id)
        elif not str(session_id).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'session_id')

        if user_id is not None and not isinstance(user_id, str):
            raise ArgumentTypeException(self.__init__, 'user_id', user_id)
        elif user_id is not None and not str(user_id).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'user_id')

        if user_tier is not None and not isinstance(user_tier, dict):
            raise ArgumentTypeException(self.__init__, 'user_tier', user_tier)

        self.__session_id = str(session_id).strip()
        self.__user_id = str(user_id).strip() if user_id else None
        self.__user_tier = user_tier if user_id else None
        self.__raw_product_data = raw_product_data
        self.__is_sold_out = (sum([int(size.get('qty') or 0) for size in raw_product_data.get('sizes', [])]) == 0)
        self.__created_at = get_mpc_datetime_now()
        self.__version = weight_version
        self.__personalize_score = personalize_score
        self.__personalize_weight = personalize_weight
        self.__question_score = question_score
        self.__question_weight = question_weight
        self.__order_score = order_score
        self.__order_weight = order_weight
        self.__tracking_score = tracking_score
        self.__tracking_weight = tracking_weight

    @property
    def session_id(self) -> str:
        return self.__session_id

    @property
    def user_id(self) -> Optional[str]:
        return self.__user_id

    @property
    def user_tier(self) -> Optional[dict]:
        return clone(self.__user_tier)

    @property
    def raw_product_data(self) -> dict:
        return clone(self.__raw_product_data)

    @property
    def config_sku(self) -> str:
        return self.__raw_product_data.get('rs_sku')

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @property
    def version(self) -> int:
        return self.__version

    @property
    def personalize_score(self) -> float:
        return self.__personalize_score

    @property
    def personalize_weight(self) -> float:
        return self.__personalize_weight

    @property
    def question_score(self) -> float:
        return self.__question_score

    @property
    def question_weight(self) -> float:
        return self.__question_weight

    @property
    def order_score(self) -> float:
        return self.__order_score

    @property
    def order_weight(self) -> float:
        return self.__order_weight

    @property
    def tracking_score(self) -> float:
        return self.__tracking_score

    @property
    def tracking_weight(self) -> float:
        return self.__tracking_weight

    @property
    def is_sold_out(self) -> bool:
        return self.__is_sold_out
    
    @property
    def total_score(self) -> float:
        try:
            value = sum([
                float(self.personalize_score) * float(self.personalize_weight),
                float(self.question_score) * float(self.question_weight),
                float(self.order_score) * float(self.order_weight),
                float(self.tracking_score) * float(self.tracking_weight),
            ])
            return float("%.2f" % value)
        except Exception as e:
            print(str(e))
            return None

    @property
    def scores(self) -> dict:
        return {
            'version': self.version,
            'ps': self.personalize_score,
            'pw': self.personalize_weight,
            'qs': self.question_score,
            'qw': self.question_weight,
            'rs': self.order_score,
            'rw': self.order_weight,
            'ts': self.tracking_score,
            'tw': self.tracking_weight,
            'total_score': self.total_score
        }

    def to_dict(self) -> dict:
        return {
            'config_sku': self.config_sku,
            'session_id': self.session_id,
            'user_id': self.user_id,
            'user_tier': self.user_tier,
            'action_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'version': self.version,
            'personalize_score': self.personalize_score,
            'personalize_weight': self.personalize_weight,
            'question_score': self.question_score,
            'question_weight': self.question_weight,
            'order_score': self.order_score,
            'order_weight': self.order_weight,
            'tracking_score': self.tracking_score,
            'tracking_weight': self.tracking_weight,
            'product_score': self.total_score,
            'is_sold_out': self.is_sold_out,
        }

    @property
    def action_data(self) -> dict:
        return {
            'version': self.version,
            'ps': self.personalize_score,
            'pw': self.personalize_weight,
            'qs': self.question_score,
            'qw': self.question_weight,
            'rs': self.order_score,
            'rw': self.order_weight,
            'ts': self.tracking_score,
            'tw': self.tracking_weight,
            'product_score': self.total_score,
            'is_sold_out': self.is_sold_out,
        }


class _ProductListAction(_BaseAction):
    def __init__(
        self,
        raw_product_data: dict,
        position_on_page: int,
        session_id: str,
        user_id: Optional[str] = None,
        user_tier: Optional[dict] = None,
        weight_version: int = None,
        personalize_score: float = None,
        personalize_weight: float = None,
        question_score: float = None,
        question_weight: float = None,
        order_score: float = None,
        order_weight: float = None,
        tracking_score: float = None,
        tracking_weight: float = None,
    ):
        if not isinstance(position_on_page, int):
            raise ArgumentTypeException(self.__init__, 'position_on_page', position_on_page)
        elif position_on_page <= 0:
            raise ArgumentValueException('{} expects {} > 0, but {} is given!'.format(
                self.__init__.__qualname__,
                'position_on_page',
                position_on_page
            ))

        super().__init__(
            raw_product_data,
            session_id,
            user_id,
            user_tier,
            weight_version=weight_version,
            personalize_score=personalize_score,
            personalize_weight=personalize_weight,
            question_score=question_score,
            question_weight=question_weight,
            order_score=order_score,
            order_weight=order_weight,
            tracking_score=tracking_score,
            tracking_weight=tracking_weight
        )

        self.__position_on_page = position_on_page

    @property
    def position_on_page(self) -> int:
        return self.__position_on_page

    @property
    def action_data(self) -> dict:
        action_data = super().action_data
        action_data['position_on_page'] = self.position_on_page
        return action_data

    def to_dict(self) -> dict:
        result = super(_ProductListAction, self).to_dict()
        result['position_on_page'] = self.position_on_page
        return result


class ViewAction(_ProductListAction):
    pass


class ClickAction(_ProductListAction):
    pass


class VisitAction(_BaseAction):
    pass


class ProductsTrackingModel(object):
    __STORE_IN_DATA_LAKE__: bool = True
    # TODO: Please be careful with this.
    __STORE_IN_ES__: bool = False  # True

    def __init__(
            self,
            store_in_data_lake: bool = True,
            store_in_elasticsearch: bool = False):
        self.__STORE_IN_DATA_LAKE__ = store_in_data_lake
        self.__STORE_IN_ES__ = store_in_elasticsearch
        self.__counters = Elastic(
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_COUNTERS,
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_COUNTERS
        )
        self.__user_actions = Elastic(
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION,
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION
        )
        self.__user_action_snapshots = Elastic(
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION_PRODUCT_SNAPSHOT,
            settings.AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION_PRODUCT_SNAPSHOT
        )

    # ------------------------------------------------------------------------------------------------------------------
    #                                                  WRITE
    # ------------------------------------------------------------------------------------------------------------------

    def sync_guest_user_actions(self, session_id: str, user_id: str, user_tier: Optional[dict]) -> None:
        if not isinstance(session_id, str):
            raise ArgumentTypeException(self.sync_guest_user_actions, 'session_id', session_id)
        elif not str(session_id).strip():
            raise ArgumentCannotBeEmptyException(self.sync_guest_user_actions, 'session_id')

        if user_id is not None and not isinstance(user_id, str):
            raise ArgumentTypeException(self.sync_guest_user_actions, 'user_id', user_id)
        elif user_id is not None and not str(user_id).strip():
            raise ArgumentCannotBeEmptyException(self.sync_guest_user_actions, 'user_id')

        if user_tier is not None and not isinstance(user_tier, dict):
            raise ArgumentTypeException(self.sync_guest_user_actions, 'user_tier', user_tier)

        session_id = str(session_id).strip()
        user_id = str(user_id).strip() if user_id else None
        user_tier = user_tier if user_id else None

        self.__user_actions.update_by_query({
            "query": {
                "bool": {
                    "must": [
                        {"match": {"session_id": session_id}},
                    ],
                    "must_not": [
                        {"exists": {"field": "user_id"}},
                    ],
                }
            },
            "script": {
                "source": "ctx._source['user_id'] = params.user_id; ctx._source['user_tier'] = params.user_tier",
                "params": {
                    "user_id": user_id,
                    "user_tier": user_tier,
                }
            }
        })

    def track(self, action_or_list: Union[_BaseAction, List[_BaseAction]]) -> None:
        actions_map = {
            ViewAction: {
                'type': ACTION_TYPE.view,
                'counter_name': 'views',
            },
            ClickAction: {
                'type': ACTION_TYPE.click,
                'counter_name': 'views',
            },
            VisitAction: {
                'type': ACTION_TYPE.visit,
                'counter_name': 'views',
            },
        }

        if self.__STORE_IN_DATA_LAKE__:
            datalake = DataLakeBase()
            buffer = list()

        if isinstance(action_or_list, _BaseAction):
            action = action_or_list
            if action.__class__ not in actions_map.keys():
                raise TypeError('{} does not know, how to work with {} instance!'.format(
                    self.track.__qualname__,
                    action.__class__.__qualname__
                ))

            if self.__STORE_IN_DATA_LAKE__:
                item = action.to_dict()
                item.update({
                    'action': actions_map[action.__class__]['type'],
                })
                buffer.append(item)
            
            if self.__STORE_IN_ES__:
                self.__track_counters(action.config_sku, actions_map[action.__class__]['counter_name'])
                self.__track_action(action, actions_map[action.__class__]['type'])
        elif isinstance(action_or_list, list) and all(isinstance(x, _BaseAction) for x in action_or_list):
            for action in action_or_list:
                if action.__class__ not in actions_map.keys():
                    print('{} does not know, how to work with {} instance!'.format(
                        self.track.__qualname__,
                        action.__class__.__qualname__
                    ))
                    continue

                if self.__STORE_IN_DATA_LAKE__:
                    item = action.to_dict()
                    item.update({
                        'action': actions_map[action.__class__]['type'],
                    })
                    buffer.append(item)
                
                if self.__STORE_IN_ES__:
                    self.__track_counters(action.config_sku, actions_map[action.__class__]['counter_name'])
                    self.__track_action(action, actions_map[action.__class__]['type'])
        else:
            raise ArgumentTypeException(self.track, 'action', action)

        if self.__STORE_IN_DATA_LAKE__:
            status, msg = datalake.put_record_batch(buffer)
            if not status:
                print(msg)

    def __track_counters(self, config_sku: str, counter_name: str):
        counters_upsert = {
            'config_sku': config_sku,
            'views': 0,
            'clicks': 0,
            'visits': 0,
        }
        counters_upsert[counter_name] += 1
        self.__counters.update_data(config_sku, {
            'script': 'ctx._source.{} += 1'.format(counter_name),
            'upsert': counters_upsert
        })

    def __track_action(self, action: _BaseAction, action_type: str) -> None:
        snapshot_id = self.__get_or_create_product_snapshot(action.raw_product_data)
        self.__user_actions.create(str(uuid.uuid4()), {
            'config_sku': action.config_sku,
            'snapshot_id': snapshot_id,
            'session_id': action.session_id,
            'user_id': action.user_id,
            'user_tier': action.user_tier,
            'action': action_type,
            'action_at': action.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'action_data': action.action_data
        })

    def __get_or_create_product_snapshot(self, raw_product_data: dict) -> str:
        config_sku = raw_product_data.get('rs_sku')

        # convert to the single format
        updated_at = raw_product_data.get('updated_at')
        if not config_sku or not updated_at:
            raise ValueError('Product data is incorrect!')
        updated_at_format = '%Y-%m-%d %H:%M:%S.%f' if '.' in updated_at else '%Y-%m-%d %H:%M:%S'
        updated_at = datetime.strptime(updated_at, updated_at_format)

        product_snapshot = self.__user_action_snapshots.post_search({
            'query': {
                'bool': {
                    'must': [
                        {'term': {'config_sku': config_sku}},
                        {'term': {'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S')}}
                    ]
                }
            }
        }).get('hits', {}).get('hits', [{}]) or [{}]
        product_snapshot = product_snapshot[0].get('_source', None) or None
        if not product_snapshot:
            product_snapshot = self.__create_product_snapshot(config_sku, updated_at, raw_product_data)
            self.__user_action_snapshots.create(product_snapshot.get('snapshot_id'), product_snapshot)

        product_snapshot_id = product_snapshot.get('snapshot_id')
        return product_snapshot_id

    def __create_product_snapshot(self, config_sku: str, updated_at: datetime, raw_product_data: dict) -> dict:
        simple_copy_attributes = [
            'event_code',

            'manufacturer',
            'product_size_attribute',
            'rs_product_sub_type',
            'rs_colour',
            'gender',
            'season',

            'size_chart',
            'neck_type',
            'fit',
            'dimensions',
            'fabrication',
            'size_fit',
            'sticker_id',

            'product_name',
            'product_description',

            'freebie',
            'rs_selling_price',
            'discount',

            'status',
        ]

        snapshot_data = {}
        for attribute in simple_copy_attributes:
            snapshot_data[attribute] = raw_product_data.get(attribute)

        snapshot_data['sizes'] = []
        for product_size_data in raw_product_data.get('sizes', []):
            snapshot_data['sizes'].append({
                'size': product_size_data.get('size'),
                'rs_simple_sku': product_size_data.get('rs_simple_sku'),
            })

        snapshot_data['images'] = []
        for product_image_data in raw_product_data.get('images', []):
            snapshot_data['images'].append({
                's3_filepath': product_image_data.get('s3_filepath'),
                'position': product_image_data.get('position'),
            })

        total_qty = sum([int(size.get('qty') or 0) for size in raw_product_data.get('sizes', [])])
        snapshot_data['is_sold_out'] = (total_qty == 0)

        return {
            'config_sku': config_sku,
            'snapshot_id': str(uuid.uuid4()),
            'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
            'data': snapshot_data
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                  READ
    # ------------------------------------------------------------------------------------------------------------------

    def get_products_counters(self, config_sku_list: List[str]) -> dict:
        """
        :param config_sku_list: [ config_sku_1, ... ]
        :return: {
            config_sku_1: {
                views: int,
                visits: int,
                clicks: int,
            },
            ...
        }
        """

        if (
            not isinstance(config_sku_list, list)
            or sum([(isinstance(cs, str) and bool(str(cs).strip())) for cs in config_sku_list]) != len(config_sku_list)
        ):
            raise ArgumentTypeException(self.get_products_user_read_status, 'config_sku_list', config_sku_list)

        if not config_sku_list:
            return {}

        _response_items = self.__counters.post_search({
            'query': {
                "terms": {"config_sku": config_sku_list},
            },
            'size': len(config_sku_list)
        }).get('hits', {}).get('hits', []) or []

        result = {}
        for _item in _response_items:
            config_sku = _item.get('_source').get('config_sku')
            result[config_sku] = {
                'views': int(_item.get('_source').get('views') or '0'),
                'visits': int(_item.get('_source').get('visits') or '0'),
                'clicks': int(_item.get('_source').get('clicks') or '0'),
            }

        for config_sku in config_sku_list:
            result[config_sku] = result[config_sku] if config_sku in result.keys() else {
                'views': 0,
                'visits': 0,
                'clicks': 0,
            }

        return result

    def get_products_user_read_status(
        self,
        config_sku_list: List[str],
        user_id: Optional[str],
        session_id: Optional[str]
    ) -> dict:
        """
        :param config_sku_list: [ config_sku_1, ... ]
        :param user_id: - is required, if session_id is empty
        :param session_id: - is required, if user_id is empty
        :return: {
            config_sku_1: bool,
            ...
        }
        """

        if (
            not isinstance(config_sku_list, list)
            or sum([(isinstance(cs, str) and bool(str(cs).strip())) for cs in config_sku_list]) != len(config_sku_list)
        ):
            raise ArgumentTypeException(self.get_products_user_read_status, 'config_sku_list', config_sku_list)

        if user_id is not None and not isinstance(user_id, str):
            raise ArgumentTypeException(self.get_products_user_read_status, 'user_id', user_id)
        if session_id is not None and not isinstance(session_id, str):
            raise ArgumentTypeException(self.get_products_user_read_status, 'session_id', session_id)

        user_id = (str(user_id).strip() if user_id is not None else None) or None
        session_id = (str(session_id).strip() if session_id is not None else None) or None
        if not user_id and not session_id:
            raise ArgumentValueException('{} expects {} or {}, but both are empty!'.format(
                self.get_products_user_read_status.__qualname__,
                'user_id',
                'session_id'
            ))

        if not config_sku_list:
            return {}

        _response = self.__user_actions.post_search({
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"config_sku": config_sku_list}},

                        # this is the same as OR condition with allowed NULL values,
                        # but this is simpler to write for elastic
                        {"term": {"user_id": user_id} if user_id else {"session_id": session_id}},
                        {"term": {"session_id": session_id} if session_id else {"user_id": user_id}},
                    ],
                }
            },
            "size": 0,
            "aggregations": {
                "config_sku_list": {
                    "terms": {
                        "field": "config_sku",
                        "size": len(config_sku_list)
                    }
                }
            }
        })

        result = {}

        for _item in _response.get('aggregations').get('config_sku_list').get('buckets'):
            result[_item.get('key')] = _item.get('doc_count') > 0

        for config_sku in config_sku_list:
            result[config_sku] = result[config_sku] if config_sku in result.keys() else False

        return result

    def get_visited_products_aggregation_data(self, user_id: str) -> dict:
        """
        Returns {
            "brands": list[str],
            "product_types": list[str],
            "product_sub_types": list[str],
            "genders": list[str],
            "sizes": list[str]
        }
        """

        if not isinstance(user_id, str):
            raise ArgumentTypeException(self.get_visited_products_aggregation_data, 'user_id', user_id)
        elif not str(user_id).strip():
            raise ArgumentCannotBeEmptyException(self.get_visited_products_aggregation_data, 'user_id')

        user_id = str(user_id).strip()
        visited_skus = self.__user_actions.post_search({
            "query": {
                "bool": {
                    "must": [
                        {"term": {"user_id": user_id}},
                        {"terms": {"action": ["visit", "click"]}}
                    ]
                }
            },
            "aggs": {
                "unique_skus": {
                    "terms": {
                        "field": "config_sku"
                    }
                }
            },
            "size": 0
        }).get('aggregations').get('unique_skus').get('buckets')
        visited_skus = [item.get('key') for item in visited_skus]

        result = {
            "brands": [],
            "product_types": [],
            "product_sub_types": [],
            "genders": [],
            "sizes": []
        }

        chunk_size = 1000
        visited_skus_chunks = [visited_skus[i:i + chunk_size] for i in range(0, len(visited_skus), chunk_size)]
        for visited_skus_chunk in visited_skus_chunks:
            chunk_aggregations = self.__user_action_snapshots.post_search({
                "query": {
                    "bool": {
                        "must": {
                            "terms": {"config_sku": visited_skus_chunk}
                        }
                    }
                },
                "aggs": {
                    "brands": {"terms": {"field": "data.manufacturer"}},
                    "product_types": {"terms": {"field": "data.product_size_attribute"}},
                    "product_sub_types": {"terms": {"field": "data.rs_product_sub_type"}},
                    "genders": {"terms": {"field": "data.gender"}},
                    "sizes": {"terms": {"field": "data.sizes.size"}}
                }
            }).get('aggregations')

            for attribute in ["brands", "product_types", "product_sub_types", "genders", "sizes"]:
                chunk_values = [item.get('key') for item in chunk_aggregations.get(attribute).get('buckets')]
                result[attribute].extend(chunk_values)
                result[attribute] = list(set(result[attribute]))

        return result

    def retrieve_logs_from_es(
            self,
            from_date: Union[str, datetime],
            to_date: Union[str, datetime],
            size=50):
        """
        - from_date: 2019-10-24
        - to_date: 2020-01-12
        - @return: []
        """
        format_string = '%Y-%m-%d %H:%M:%S'
        weight_model = WeightModel()
        weights = weight_model.retrieve_weights(from_date, to_date)
        def get_datetime_from_str(param: str):
            try:
                return datetime.strptime(param, format_string)
            except:
                return get_mpc_datetime_now()

        if isinstance(from_date, str):
            from_date = '%s 00:00:00' % from_date
            from_date = get_datetime_from_str(from_date)
        if isinstance(to_date, str):
            to_date = '%s 23:59:59' % to_date
            to_date = get_datetime_from_str(to_date)
        _response = self.__user_actions.post_search({
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "action_at": {
                                    "gte": from_date.strftime(format_string),
                                    "lte": to_date.strftime(format_string)
                                }
                            }
                        },
                        {
                            "exists": {
                                "field": "action_data.pw.keyword"
                            }
                        }
                    ]
                }
            },
            "size": size,
        })
        return _response['hits']

    def get_click_through_report(
            self,
            from_date: Union[str, datetime],
            to_date: Union[str, datetime]):
        """
        - from_date: 2019-10-24
        - to_date: 2020-01-12
        - @return: []
        """
        format_string = '%Y-%m-%d %H:%M:%S'
        weight_model = WeightModel()
        weights = weight_model.retrieve_weights(from_date, to_date)
        def get_datetime_from_str(param: str):
            try:
                return datetime.strptime(param, format_string)
            except:
                return get_mpc_datetime_now()

        if isinstance(from_date, str):
            from_date = '%s 00:00:00' % from_date
            from_date = get_datetime_from_str(from_date)
        if isinstance(to_date, str):
            to_date = '%s 23:59:59' % to_date
            to_date = get_datetime_from_str(to_date)

        _response = self.__user_actions.post_search({
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "action_at": {
                                    "gte": from_date.strftime(format_string),
                                    "lte": to_date.strftime(format_string)
                                }
                            }
                        }
                    ]
                }
            },
            "size": 0, 
            "aggs": {
                "users": {
                    "terms": {
                        "field": "user_id",
                        "size": 1000
                    },
                    "aggs": {
                        "config_sku_list": {
                            "terms": {
                                "field": "config_sku",
                                "size": 1000
                            },
                            "aggs": {
                                "pws": {
                                    "terms": {
                                        "field": "action_data.pw.keyword"
                                    },
                                    "aggs": {
                                        "qws": {
                                            "terms": {
                                                "field": "action_data.qw.keyword"
                                            },
                                            "aggs": {
                                                "rws": {
                                                    "terms": {
                                                        "field": "action_data.rw.keyword"
                                                    },
                                                    "aggs": {
                                                        "tws": {
                                                            "terms": {
                                                                "field": "action_data.tw.keyword"
                                                            },
                                                            "aggs": {
                                                                "actions": {
                                                                    "terms": {
                                                                        "field": "action"
                                                                    }
                                                                },
                                                                "sold_out_views": {
                                                                    "filter": {
                                                                        "bool": {
                                                                            "must": [
                                                                                {
                                                                                    "term": {
                                                                                        "action_data.is_sold_out": True
                                                                                    }
                                                                                },
                                                                                {
                                                                                    "term": {
                                                                                        "action": {
                                                                                            "value": "view"
                                                                                        }
                                                                                    }
                                                                                }
                                                                            ]
                                                                        }
                                                                    }
                                                                },
                                                                "sold_out_clicks": {
                                                                    "filter": {
                                                                        "bool": {
                                                                            "must": [
                                                                                {
                                                                                    "term": {
                                                                                        "action_data.is_sold_out": True
                                                                                    }
                                                                                },
                                                                                {
                                                                                    "terms": {
                                                                                        "action": ["click", "visit"]
                                                                                    }
                                                                                }
                                                                            ]
                                                                        }
                                                                    }
                                                                },
                                                                "min_product_position": {
                                                                    "min": {
                                                                        "field": "action_data.position_on_page"
                                                                    }
                                                                },
                                                                "max_product_position": {
                                                                    "max": {
                                                                        "field": "action_data.position_on_page"
                                                                    }
                                                                },
                                                                "mean_product_position": {
                                                                    "avg": {
                                                                        "field": "action_data.position_on_page"
                                                                    }
                                                                },
                                                                "median_product_position": {
                                                                    "median_absolute_deviation": {
                                                                        "field": "action_data.position_on_page"
                                                                    }
                                                                },
                                                                "min_product_score": {
                                                                    "min": {
                                                                        "field": "action_data.product_score"
                                                                    }
                                                                },
                                                                "max_product_score": {
                                                                    "max": {
                                                                        "field": "action_data.product_score"
                                                                    }
                                                                },
                                                                "mean_product_score": {
                                                                    "avg": {
                                                                        "field": "action_data.product_score"
                                                                    }
                                                                },
                                                                "median_product_score": {
                                                                    "median_absolute_deviation": {
                                                                        "field": "action_data.product_score"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })

        result = list()
        for user_bucket in _response['aggregations']['users']['buckets']:
            user_id = user_bucket['key']
            for config_sku_bucket in user_bucket['config_sku_list']['buckets']:
                config_sku = config_sku_bucket['key']
                for pw_bucket in config_sku_bucket['pws']['buckets']:
                    pw = pw_bucket['key']
                    for qw_bucket in pw_bucket['qws']['buckets']:
                        qw = qw_bucket['key']
                        for rw_bucket in qw_bucket['rws']['buckets']:
                            rw = rw_bucket['key']
                            for tw_bucket in rw_bucket['tws']['buckets']:
                                tw = tw_bucket['key']
                                actions = {'view': 0, 'click': 0}
                                for action_bucket in tw_bucket['actions']['buckets']:
                                    action_type = action_bucket['key']
                                    if action_type == 'visit':
                                        action_type = 'click'
                                    actions.update({
                                        action_type: action_bucket['doc_count']
                                    })
                                result.append({
                                    'user_id': user_id,
                                    'config_sku': config_sku,
                                    'pw': pw,
                                    'qw': qw,
                                    'rw': rw,
                                    'tw': tw,
                                    'views': actions['view'],
                                    'clicks': actions['click'],
                                    'sold_out_views': tw_bucket['sold_out_views']['doc_count'],
                                    'sold_out_clicks': tw_bucket['sold_out_clicks']['doc_count'],
                                    'min_product_position': tw_bucket['min_product_position']['value'],
                                    'max_product_position': tw_bucket['max_product_position']['value'],
                                    'mean_product_position': tw_bucket['mean_product_position']['value'],
                                    'median_product_position': tw_bucket['median_product_position']['value'],
                                    'min_product_score': tw_bucket['min_product_score']['value'],
                                    'max_product_score': tw_bucket['max_product_score']['value'],
                                    'mean_product_score': tw_bucket['mean_product_score']['value'],
                                    'median_product_score': tw_bucket['median_product_score']['value']
                                })

        return result

# ----------------------------------------------------------------------------------------------------------------------

