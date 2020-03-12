import math
from typing import Optional, Union, List, Tuple
from datetime import datetime, timedelta
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from .product_visit_logs import ProductVisitLog
from .ProductSizeSort import ProductSizeSort
from ..ml.product_entry import ProductEntry


# ----------------------------------------------------------------------------------------------------------------------


class ProductSearchCriteria(object):
    __DEFAULT_PAGE_SIZE = 18

    __PROPERTY_SEARCH_QUERY = '__search_query'
    __PROPERTY_GENDER_NAME = '__gender_name'
    __PROPERTY_MANUFACTURER_NAME = '__manufacturer_name'
    __PROPERTY_SIZE_NAME = '__size_name'
    __PROPERTY_COLOR_NAME = '__color_name'
    __PROPERTY_SUB_TYPE_NAME = '__sub_type_name'
    __PROPERTY_PRICE_MIN = '__price_min'
    __PROPERTY_PRICE_MAX = '__price_max'
    __NAME_PROPERTIES = (
        __PROPERTY_GENDER_NAME,
        __PROPERTY_MANUFACTURER_NAME,
        __PROPERTY_SIZE_NAME,
        __PROPERTY_COLOR_NAME,
        __PROPERTY_SUB_TYPE_NAME,
    )

    SORT_COLUMN_ID = 'id'
    SORT_COLUMN_SKU = 'sku'
    SORT_COLUMN_TITLE = 'title'
    SORT_COLUMN_SUB_TITLE = 'subtitle'
    SORT_COLUMN_PRICE = 'price'
    SORT_COLUMN_TYPE = 'product_type'
    SORT_COLUMN_SUB_TYPE = 'product_sub_type'
    SORT_COLUMN_GENDER = 'gender'
    SORT_COLUMN_BRAND = 'brand'
    SORT_COLUMN_SIZE = 'size'
    SORT_COLUMN_COLOR = 'color'
    SORT_COLUMN_CREATED_AT = 'newin'
    SORT_COLUMN_SCORE = '_score'
    SORT_COLUMN_PERCENTAGE_SCORE = 'percentage_score'
    __SORT_COLUMNS = (
        SORT_COLUMN_ID,
        SORT_COLUMN_SKU,
        SORT_COLUMN_TITLE,
        SORT_COLUMN_SUB_TITLE,
        SORT_COLUMN_PRICE,
        SORT_COLUMN_TYPE,
        SORT_COLUMN_SUB_TYPE,
        SORT_COLUMN_GENDER,
        SORT_COLUMN_BRAND,
        SORT_COLUMN_SIZE,
        SORT_COLUMN_COLOR,
        SORT_COLUMN_CREATED_AT,
        SORT_COLUMN_SCORE,
        SORT_COLUMN_PERCENTAGE_SCORE,
    )

    SORT_DIRECTION_ASC = 'asc'
    SORT_DIRECTION_DESC = 'desc'
    __SORT_DIRECTIONS = (
        SORT_DIRECTION_ASC,
        SORT_DIRECTION_DESC,
    )

    @classmethod
    def __set_property(cls, self, property_name, value):
        setattr(self, '_' + cls.__name__ + property_name, value)

    @classmethod
    def __get_property(cls, self, property_name):
        return getattr(self, '_' + cls.__name__ + property_name)

    def __init__(self):
        self.__page_number = 1
        self.__page_size = self.__class__.__DEFAULT_PAGE_SIZE
        self.__sort_column = self.__class__.SORT_COLUMN_SCORE
        self.__sort_direction = self.__class__.SORT_DIRECTION_ASC
        self.__class__.__set_property(self, self.__class__.__PROPERTY_SEARCH_QUERY, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_GENDER_NAME, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_MANUFACTURER_NAME, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_PRICE_MIN, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_PRICE_MAX, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_SIZE_NAME, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_COLOR_NAME, None)
        self.__class__.__set_property(self, self.__class__.__PROPERTY_SUB_TYPE_NAME, None)

    @property
    def page_number(self) -> int:
        return self.__page_number

    @property
    def page_size(self) -> int:
        return self.__page_size

    @property
    def sort_column(self) -> str:
        return self.__sort_column

    @property
    def sort_direction(self) -> str:
        return self.__sort_direction

    @property
    def search_query(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_SEARCH_QUERY)

    @property
    def gender_name(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_GENDER_NAME)

    @property
    def manufacturer_name(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_MANUFACTURER_NAME)

    @property
    def size_name(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_SIZE_NAME)

    @property
    def color_name(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_COLOR_NAME)

    @property
    def sub_type_name(self) -> Optional[str]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_SUB_TYPE_NAME)

    @property
    def price_min(self) -> Optional[float]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_PRICE_MIN)

    @property
    def price_max(self) -> Optional[float]:
        return self.__class__.__get_property(self, self.__class__.__PROPERTY_PRICE_MAX)

    def set_page(self, page_number: int, page_size: int = None) -> None:
        if not isinstance(page_number, int):
            raise ArgumentTypeException(self.set_page, 'page_number', page_number)
        elif page_number < 1:
            raise ArgumentValueException('Page Number must be > 0!')

        page_size = page_size if page_size is not None else self.__class__.__DEFAULT_PAGE_SIZE
        if not isinstance(page_size, int):
            raise ArgumentTypeException(self.page_size, 'value', page_size)
        elif page_size < 1:
            raise ArgumentValueException('Page Size must be > 0!')

        self.__page_number = page_number
        self.__page_size = page_size

    def set_sort(self, sort_column: str, sort_direction: str = None) -> None:
        if sort_column not in self.__class__.__SORT_COLUMNS:
            raise ArgumentUnexpectedValueException(sort_column, self.__class__.__SORT_COLUMNS)

        sort_direction = sort_direction if sort_direction is not None else self.__class__.__SORT_DIRECTIONS[0]
        if sort_direction not in self.__class__.__SORT_DIRECTIONS:
            raise ArgumentUnexpectedValueException(sort_direction, self.__class__.__SORT_DIRECTIONS)

        self.__sort_column = sort_column
        self.__sort_direction = sort_direction

    def __set_name_parameter(self, property_name: str, value: Optional[str]) -> None:
        if not isinstance(property_name, str):
            raise ArgumentTypeException(self.__set_name_parameter, 'property_name', property_name)
        
        if property_name not in self.__class__.__NAME_PROPERTIES:
            raise ArgumentUnexpectedValueException(property_name, self.__class__.__NAME_PROPERTIES)

        if value is not None and not isinstance(value, str):
            raise ArgumentTypeException(self.__set_name_parameter, 'value', value)

        value = None if value is None else str(value).strip() or None

        self.__class__.__set_property(self, property_name, value)

    def set_search_query(self, value: Optional[str]) -> None:
        self.__class__.__set_property(self, self.__class__.__PROPERTY_SEARCH_QUERY, value)

    def set_gender_name(self, value: Optional[str]) -> None:
        self.__set_name_parameter(self.__class__.__PROPERTY_GENDER_NAME, value)

    def set_manufacturer_name(self, value: Optional[str]) -> None:
        self.__set_name_parameter(self.__class__.__PROPERTY_MANUFACTURER_NAME, value)

    def set_size_name(self, value: Optional[str]) -> None:
        self.__set_name_parameter(self.__class__.__PROPERTY_SIZE_NAME, value)

    def set_color_name(self, value: Optional[str]) -> None:
        self.__set_name_parameter(self.__class__.__PROPERTY_COLOR_NAME, value)

    def set_sub_type_name(self, value: Optional[str]):
        self.__set_name_parameter(self.__class__.__PROPERTY_SUB_TYPE_NAME, value)

    def set_price_range(self, price_min: Union[int, float, None], price_max: Union[int, float, None]) -> None:
        self.__set_price_range_min(price_min)
        self.__set_price_range_max(price_max)

    def __set_price_range_min(self, price_min: Union[int, float, None]) -> None:
        if price_min is None:
            self.__price_min = None
            return

        if not isinstance(price_min, int) and not isinstance(price_min, float):
            raise ArgumentTypeException(self.__set_price_range_min, 'price_min', price_min)
        elif price_min < 0:
            raise ArgumentValueException('Price Min cannot be < 0!')

        self.__price_min = float(str(price_min))

    def __set_price_range_max(self, price_max: Union[int, float, None]) -> None:
        if price_max is None:
            self.__price_max = None
            return

        if not isinstance(price_max, int) and not isinstance(price_max, float):
            raise ArgumentTypeException(self.__set_price_range_max, 'price_max', price_max)
        elif price_max < 0:
            raise ArgumentValueException('Price Max cannot be < 0!')

        self.__price_max = float(str(price_max))


# ----------------------------------------------------------------------------------------------------------------------


class Product(object):
    # @todo : refactoring ~ get_all(criteria, limit, offset)

    def __init__(self):
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            settings.AWS_ELASTICSEARCH_PRODUCTS
        )

    def get_all(self, convert: bool = False):
        # TODO: Should be refactored later.
        offset, CHUNK_SIZE = 0, 1000
        products = []

        query = {
            "size": CHUNK_SIZE,
            "from": offset,
        }

        response = self.__elastic.post_search(query)['hits']
        total = response['total']
        products += response['hits']
        while len(products) < total:
            offset += CHUNK_SIZE
            query = {
                "size": CHUNK_SIZE,
                "from": offset,
            }
            response = self.__elastic.post_search(query)['hits']
            products += response['hits']
            
        if convert:
            return self.__convert_products({'total': total, 'hits': products})
        else:
            return [ProductEntry(**item['_source']) for item in products]

    def listAll(self, sort, order, page=1, size=18):
        fromindex = (int(page) - 1) * int(size)
        if fromindex < 0:
            fromindex = 0

        sort = self.__class__.__convert_filter(sort)
        if sort == "invalid_name":
            return {"error": "invalid sort field"}
        query = {
            "size": size,
            "from": fromindex,
            "sort": [{
                sort: {"order": order}
            }]
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response)

    def __makeESFilterFromCustomFilter(self, custom_filters: Optional[dict]):
        ret = {}
        ret['bool']={}
        ret['bool']['must']=list()
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

    def listByCustomFilter(
        self,
        custom_filters: Optional[dict],
        sorts: dict,
        tier: dict,
        page: int,
        size: int
    ):
        filters = self.__makeESFilterFromCustomFilter(custom_filters)
        fromindex = (int(page) - 1) * int(size)
        if fromindex < 0:
            fromindex = 0

        query = {
            "query": filters,
            "size": size,
            "from": fromindex,
            "sort": [
                self.__class__.__convert_sort_filter(column, direction)
                for column, direction in sorts.items()
            ],
        }

        response = self.__elastic.post_search(query)['hits']
        return self.__convert_products(response, tier=tier)

    def update(self, config_sku, data):
        json_data = {
            "doc": data
        }
        response = self.__elastic.update_data(config_sku, json_data)
        return response

    def get(
        self,
        id,
        log: bool = False,
        session_id: str = None,
        customer_id: str = None
    ):
        item = self.__elastic.get_data(id)

        if log and item is not None:
            # TODO: refactoring - move out from model's method
            log_model = ProductVisitLog(session_id, customer_id=customer_id)
            log_model.insert(self.__convert_item(item))

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

    def __convert_products(self, data, tier: dict = None):
        ret ={
            "total": data["total"],
            "products": [self.__convert_item(item["_source"], tier=tier) for item in data["hits"]]
        }
        return ret

    @staticmethod
    def __convert_item_calculate_prices(item) -> tuple:
        original_price = float(item['rs_selling_price'] or 0)
        discount = float(item['discount'] or 0)
        current_price = original_price - original_price * discount / 100
        return original_price, current_price

    def __convert_item(self, item, tier: dict = None):
        original_price, current_price = self.__class__.__convert_item_calculate_prices(item)

        fbucks = None
        if isinstance(tier, dict) and not tier.get('is_neutral'):
            fbucks = math.ceil(item.get('current_price', current_price) * tier['discount_rate'] / 100)

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
            }
        }

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
            # 'newin': 'created_at',
            '_score': '_score',
            'search_query': 'search_query'
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
            '_score': {'_score': {'order': direction}},
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
            }
        }

        if column_name not in sort_map.keys():
            raise ValueError('Oh, no! {} does not know, how to {} with {} column!'.format(
                Product.__qualname__,
                '__convert_sort_filter',
                column_name
            ))

        return sort_map[column_name]

    def __getSumofQTY(self, data):
        filters = self.__makeESFilterFromCustomFilter(data)
        query = {
            "query": filters,
            "size": 0,
            "aggregations": {
                "count": {
                    "sum": {
                        "field": "sizes.qty"
                    }
                }
            }
        }
        aggregations = self.__elastic.post_search(query)['aggregations']
        return aggregations['count']['value']

    def __getAvailableSubType(self, data, sort):
        filters = self.__makeESFilterFromCustomFilter(data)
        query = {
            "query": filters,
            "size": 0,
            "aggregations": {
                "product_sub_type": {
                    "terms": {
                        "field": "rs_product_sub_type",
                        "size": 100000
                    }
                }
            }
        }
        aggregations = self.__elastic.post_search(query)['aggregations']
        sub_types = []
        for item in aggregations['product_sub_type']['buckets']:
            sub_types.append(item['key'])
        if sort == 'asc':
            sub_types.sort()
        elif sort == 'desc':
            sub_types.sort(reverse = True)
        ret = []

        for sub_type in sub_types:
            new_filter = data.copy()
            new_filter['product_sub_type']=sub_type
            ret.append({
                'label': sub_type,
                # 'count': self.__getSumofQTY(new_filter)
            })
        return ret

    # This is too slow.
    def getNewAvailableFilter(self, data, sort):
        filters = self.__makeESFilterFromCustomFilter(data)
        query = {
            "query": filters,
            "size": 0,
            "aggregations": {
                "gender": {
                    "terms": {
                        "field": "gender",
                        "size": 100000
                    }
                },
                "price": {
                    "terms": {
                        "field": "rs_selling_price",
                        "size": 100000
                    }
                },
                "product_type": {
                    "terms": {
                        "field": "product_size_attribute",
                        "size": 100000
                    }
                },
                "brand": {
                    "terms": {
                        "field": "manufacturer",
                        "size": 100000
                    }
                },
                "size": {
                    "terms": {
                        "field": "sizes.size",
                        "size": 100000
                    }
                },
                "color": {
                    "terms": {
                        "field": "rs_colour",
                        "size": 100000
                    }
                }
            }
        }
        aggregations = self.__elastic.post_search(query)['aggregations']
        availablefilter = {
            'product_type': [],
            'brand': [],
            'size': [],
            'color': [],
            'price': [],
            'gender': []
        }
        for item in aggregations['product_type']['buckets']:
            product_type = {}
            product_type['label']=item['key']
            new_filter = data.copy()
            new_filter['product_type']=item['key']
            product_type['children'] = self.__getAvailableSubType(new_filter, sort)
            # product_type['count']=self.__getSumofQTY(new_filter)
            availablefilter['product_type'].append(product_type)
        for item in aggregations['brand']['buckets']:
            brand = {}
            brand['label']=item['key']
            new_filter = data.copy()
            new_filter['brand']=item['key']
            # brand['count']=self.__getSumofQTY(new_filter)
            availablefilter['brand'].append(brand)
        for item in aggregations['color']['buckets']:
            color = {}
            color['label']=item['key']
            new_filter = data.copy()
            new_filter['color']=item['key']
            # color['count']=self.__getSumofQTY(new_filter)
            availablefilter['color'].append(color)
        for item in aggregations['size']['buckets']:
            size = {}
            size['label']=item['key']
            availablefilter['size'].append(size)
        for item in aggregations['price']['buckets']:
            availablefilter['price'].append(item['key'])
        for item in aggregations['gender']['buckets']:
            gender = {}
            gender['label']=item['key']
            new_filter = data.copy()
            new_filter['gender']=item['key']
            # gender['count']=self.__getSumofQTY(new_filter)
            availablefilter['gender'].append(gender)

        for key, items in availablefilter.items():
            if key == 'size':
                _sizes = [item['label'] for item in items]
                _sorted_sizes = ProductSizeSort().sort(_sizes)
                if sort == 'desc':
                    _sorted_sizes.reverse()
                availablefilter['size'].clear()
                for _item in _sorted_sizes:
                    size = {}
                    size['label']=_item
                    new_filter = data.copy()
                    new_filter['size']=_item
                    # size['count']=self.__getSumofQTY(new_filter)
                    availablefilter['size'].append(size)
            elif key == 'price':
                price_list = {
                    'Under R100': 0,
                    'R100 - R250': 0,
                    'R250 - R500': 0,
                    'R500 - R750': 0,
                    'R750 - R1,000': 0,
                    'R1,000 - R2,000': 0,
                    'Over R2,000': 0
                }
                for _item in items:
                    if _item < 100:
                        price_list['Under R100'] += 1
                        continue
                    elif _item < 250:
                        price_list['R100 - R250'] += 1
                        continue
                    elif _item < 500:
                        price_list['R250 - R500'] += 1
                        continue
                    elif _item < 750:
                        price_list['R500 - R750'] += 1
                        continue
                    elif _item < 1000:
                        price_list['R750 - R1,000'] += 1
                        continue
                    elif _item < 2000:
                        price_list['R1,000 - R2,000'] += 1
                        continue
                    else:
                        price_list['R1,000 - R2,000'] += 1
                availablefilter['price'].clear()
                if sort == 'asc':
                    if price_list['Under R100'] > 0: availablefilter['price'].append({'label': 'Under R100', 'value': [0, 100]})
                    if price_list['R100 - R250'] > 0: availablefilter['price'].append({'label': 'R100 - R250', 'value': [100, 250]})
                    if price_list['R250 - R500'] > 0: availablefilter['price'].append({'label': 'R250 - R500', 'value': [250, 500]})
                    if price_list['R500 - R750'] > 0: availablefilter['price'].append({'label': 'R500 - R750', 'value': [500, 750]})
                    if price_list['R750 - R1,000'] > 0: availablefilter['price'].append({'label': 'R750 - R1,000', 'value': [750, 1000]})
                    if price_list['R1,000 - R2,000'] > 0: availablefilter['price'].append({'label': 'R1,000 - R2,000', 'value': [1000, 2000]})
                    if price_list['Over R2,000'] > 0: availablefilter['price'].append({'label': 'Over R2,000', 'value': [2000, 10000]})
                elif sort == 'desc':
                    if price_list['Over R2,000'] > 0: availablefilter['price'].append({'label': 'Over R2,000', 'value': [2000, 10000]})
                    if price_list['R1,000 - R2,000'] > 0: availablefilter['price'].append({'label': 'R1,000 - R2,000', 'value': [1000, 2000]})
                    if price_list['R750 - R1,000'] > 0: availablefilter['price'].append({'label': 'R750 - R1,000', 'value': [750, 1000]})
                    if price_list['R500 - R750'] > 0: availablefilter['price'].append({'label': 'R500 - R750', 'value': [500, 750]})
                    if price_list['R250 - R500'] > 0: availablefilter['price'].append({'label': 'R250 - R500', 'value': [250, 500]})
                    if price_list['R100 - R250'] > 0: availablefilter['price'].append({'label': 'R100 - R250', 'value': [100, 250]})
                    if price_list['Under R100'] > 0: availablefilter['price'].append({'label': 'Under R100', 'value': [0, 100]})
            else:
                if sort == 'asc':
                    availablefilter[key] = sorted(items, key = lambda i: i['label'].lower())
                elif sort == 'desc':
                    availablefilter[key] = sorted(items, key = lambda i: i['label'].lower(), reverse=True)
        return availablefilter

    def getAvailableFilter(self, data, sort):
        filters = self.__makeESFilterFromCustomFilter(data)
        query = {
            "query": filters,
            "size": 0,
            "aggregations": {
                "gender": {
                    "terms": {
                        "field": "gender",
                        "size": 100000
                    }
                },
                "price": {
                    "terms": {
                        "field": "rs_selling_price",
                        "size": 100000
                    }
                },
                "product_type": {
                    "terms": {
                        "field": "product_size_attribute",
                        "size": 100000
                    }
                },
                "product_sub_type": {
                    "terms": {
                        "field": "rs_product_sub_type",
                        "size": 100000
                    }
                },
                "brand": {
                    "terms": {
                        "field": "manufacturer",
                        "size": 100000
                    }
                },
                "size": {
                    "terms": {
                        "field": "sizes.size",
                        "size": 100000
                    }
                },
                "color": {
                    "terms": {
                        "field": "rs_colour",
                        "size": 100000
                    }
                }
            }
        }
        aggregations = self.__elastic.post_search(query)['aggregations']
        availablefilter = {
            'product_type': [],
            'product_sub_type': [],
            'brand': [],
            'size': [],
            'color': [],
            'price': [],
            'gender': []
        }
        for item in aggregations['product_type']['buckets']:
            availablefilter['product_type'].append(item['key'])
        for item in aggregations['product_sub_type']['buckets']:
            availablefilter['product_sub_type'].append(item['key'])
        for item in aggregations['brand']['buckets']:
            availablefilter['brand'].append(item['key'])
        for item in aggregations['color']['buckets']:
            availablefilter['color'].append(item['key'])
        for item in aggregations['size']['buckets']:
            availablefilter['size'].append(item['key'])
        for item in aggregations['price']['buckets']:
            availablefilter['price'].append(item['key'])
        for item in aggregations['gender']['buckets']:
            availablefilter['gender'].append(item['key'])

        for key, items in availablefilter.items():
            if key == 'size':
                sorted_sizes = ProductSizeSort().sort(items)
                if sort == 'desc':
                    sorted_sizes.reverse()
                availablefilter[key] = sorted_sizes
            else:
                if sort == 'asc':
                    items.sort()
                elif sort == 'desc':
                    items.sort(reverse = True)
        return availablefilter

    def updateStock(self, items):
        try:
            total_found_item_count = 0
            updated_item_count = 0
            added_item_count = 0
            for item in items:
                script = ("for(int j = 0; j < ctx._source.sizes.size(); j++) if(ctx._source.sizes[j].rs_simple_sku == '"
                + item['rs_simple_sku'] + "'){ ctx._source.sizes[j].qty = "
                + str(item['qty']) + "; break; }")
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

    def get_raw_data_by_skus(self, config_skus: List[str], convert=False) -> List[dict]:
        try:
            response = self.__elastic.post_search({
                'query': {
                    'terms': {'rs_sku': config_skus}
                }
            }).get('hits', {}).get('hits', [{}])
            if convert:
                return [self.__convert_item(item['_source']) for item in response]
            else:
                return [item['_source'] for item in response]
        except:
            return []
