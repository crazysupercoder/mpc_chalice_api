import json
import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.purchase.core import \
    OrderNumber, SimpleSku, Qty, \
    CancelRequest, CancelRequestStorageInterface, \
    RefundMethodAbstract
from chalicelib.libs.purchase.payment_methods.refund_methods import StoreCreditRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import EftRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import CreditCardRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import MobicredRefundMethod


class CancelRequestStorageImplementation(CancelRequestStorageInterface):
    def __init__(self):
        self.__storage = _CancelRequestStorageDynamoDb()

    def save(self, cancel_request: CancelRequest) -> None:
        self.__storage.save(cancel_request)

    def get_by_number(self, request_number: CancelRequest.Number) -> Optional[CancelRequest]:
        return self.__storage.get_by_number(request_number)

    def get_all_by_order_number(self, order_number: OrderNumber) -> Tuple[CancelRequest]:
        return self.__storage.get_all_by_order_number(order_number)


def _restore_refund_method(descriptor: str, extra_data: dict) -> RefundMethodAbstract:
    # @todo : refactoring
    # @todo : copy paste code
    methods_map = {
        'store_credit': StoreCreditRefundMethod,
        'credit_card_eft': EftRefundMethod,
        'credit_card': CreditCardRefundMethod,
        'mobicred': MobicredRefundMethod,
    }
    method_class = methods_map.get(descriptor)
    if not method_class:
        raise ValueError('{} does not know, how to work with {} delivery method!'.format(
            _restore_refund_method,
            descriptor
        ))

    return method_class(**extra_data)


class _CancelRequestStorageElastic(CancelRequestStorageInterface):
    """
        curl -X DELETE localhost:9200/purchase_cancel_requests
        curl -X PUT localhost:9200/purchase_cancel_requests -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_cancel_requests": {
                    "properties": {
                        "request_number": {"type": "keyword"},
                        "order_number": {"type": "keyword"},
                        "request_items": {
                            "properties": {
                                "simple_sku": {"type": "keyword"},
                                "qty": {"type": "integer"},
                                "status": {"type": "keyword"},
                                "processed_at": {"type": "date", "format": "date_hour_minute_second_millis"}
                            }
                        },
                        "refund_method": {"type": "keyword"},
                        "refund_method_extra_data_json": {"type": "keyword"},
                        "additional_comment": {"type": "keyword"},
                        "requested_at": {"type": "date", "format": "date_hour_minute_second_millis"}
                    }
                }
            }
        }'

        curl -X DELETE localhost:9200/purchase_cancel_requests_orders_map
        curl -X PUT localhost:9200/purchase_cancel_requests_orders_map -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_cancel_requests_orders_map": {
                    "properties": {
                        "request_numbers_json": {"type": "keyword"}
                    }
                }
            }
        }'
    """

    __ENTITY_PROPERTY_REQUEST_NUMBER = '__number'
    __ENTITY_PROPERTY_ORDER_NUMBER = '__order_number'
    __ENTITY_PROPERTY_ITEMS = '__items'
    __ENTITY_PROPERTY_ITEMS_SIMPLE_SKU = '__simple_sku'
    __ENTITY_PROPERTY_ITEMS_QTY = '__qty'
    __ENTITY_PROPERTY_ITEMS_STATUS = '__status'
    __ENTITY_PROPERTY_ITEMS_PROCESSED_AT = '__processed_at'
    __ENTITY_PROPERTY_REFUND_METHOD = '__refund_method'
    __ENTITY_PROPERTY_ADDITIONAL_COMMENT = '__additional_comment'
    __ENTITY_PROPERTY_REQUESTED_AT = '__requested_at'

    def __init__(self):
        self.__requests_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS,
            settings.AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS
        )
        self.__order_requests_map_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS_ORDERS_MAP,
            settings.AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS_ORDERS_MAP
        )
        self.__reflector = Reflector()

    def save(self, cancel_request: CancelRequest) -> None:
        if not isinstance(cancel_request, CancelRequest):
            raise ArgumentTypeException(self.save, 'cancel_request', cancel_request)

        items_data = []
        for item in cancel_request.items:
            items_data.append({
                "simple_sku": item.simple_sku.value,
                "qty": item.qty.value,
                "status": item.status.value,
                # elastic supports only 3 digits for milliseconds
                "processed_at": item.processed_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] if item.processed_at else None
            })

        document_id = cancel_request.number.value
        document_data = {
            "request_number": cancel_request.number.value,
            "order_number": cancel_request.order_number.value,
            "request_items": items_data,

            'refund_method': cancel_request.refund_method.descriptor,
            'refund_method_extra_data_json': json.dumps(cancel_request.refund_method.extra_data),

            # elastic supports only 3 digits for milliseconds
            "requested_at": cancel_request.requested_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],

            "additional_comment": cancel_request.additional_comment.value if cancel_request.additional_comment else None
        }

        existed_request = self.get_by_number(cancel_request.number)
        if existed_request:
            self.__requests_elastic.update_data(document_id, {
                'doc': document_data
            })
        else:
            self.__requests_elastic.create(document_id, document_data)

            # Elastic can search by attributes only after 1 second from last update.
            # We need all data, when we are searching by order_number,
            # so in this case we will lost fresh data, if search directly after creation of a new cancel_request.
            # In this case we need to use another index and get data by elastic doc_id.

            order_requests_map = self.__order_requests_map_elastic.get_data(cancel_request.order_number.value)
            if order_requests_map:
                request_numbers = list(json.loads(order_requests_map.get('request_numbers_json', '[]')) or [])
                request_numbers.append(cancel_request.number.value)
                request_numbers = list(set(request_numbers))
                self.__order_requests_map_elastic.update_data(cancel_request.order_number.value, {
                    'doc': {
                        'request_numbers_json': json.dumps(request_numbers)
                    }
                })
            else:
                self.__order_requests_map_elastic.create(cancel_request.order_number.value, {
                    'request_numbers_json': json.dumps([cancel_request.number.value])
                })

    def get_by_number(self, request_number: CancelRequest.Number) -> Optional[CancelRequest]:
        if not isinstance(request_number, CancelRequest.Number):
            raise ArgumentTypeException(self.get_by_number, 'request_number', request_number)

        data = self.__requests_elastic.get_data(request_number.value)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data: dict) -> CancelRequest:
        cancel_request = self.__reflector.construct(CancelRequest, {
            self.__class__.__ENTITY_PROPERTY_REQUEST_NUMBER: CancelRequest.Number(data['request_number']),
            self.__class__.__ENTITY_PROPERTY_ORDER_NUMBER: OrderNumber(data['order_number']),
            self.__class__.__ENTITY_PROPERTY_ITEMS: tuple([
                self.__reflector.construct(CancelRequest.Item, {
                    self.__class__.__ENTITY_PROPERTY_ITEMS_SIMPLE_SKU: SimpleSku(item_data['simple_sku']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_QTY: Qty(item_data['qty']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS: CancelRequest.Item.Status(item_data['status']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_PROCESSED_AT: (
                        datetime.datetime.strptime(item_data['processed_at'] + '000', '%Y-%m-%dT%H:%M:%S.%f')
                        if item_data['processed_at'] else None
                    ),
                })
                for item_data in data['request_items']
            ]),
            self.__class__.__ENTITY_PROPERTY_REFUND_METHOD: _restore_refund_method(
                data['refund_method'],
                json.loads(data['refund_method_extra_data_json'])
            ),
            self.__class__.__ENTITY_PROPERTY_ADDITIONAL_COMMENT: (
                CancelRequest.AdditionalComment(data['additional_comment'])
                if data.get('additional_comment') or None
                else None
            ),
            self.__class__.__ENTITY_PROPERTY_REQUESTED_AT: datetime.datetime.strptime(
                data['requested_at'] + '000',
                '%Y-%m-%dT%H:%M:%S.%f'
            ),
        })

        return cancel_request

    def get_all_by_order_number(self, order_number: OrderNumber) -> Tuple[CancelRequest]:
        if not isinstance(order_number, OrderNumber):
            raise ArgumentTypeException(self.get_all_by_order_number, 'order_number', order_number)

        data = self.__order_requests_map_elastic.get_data(order_number.value)
        request_numbers = json.loads((data.get('request_numbers_json') or '[]') if data else '[]') or []
        if not request_numbers:
            return tuple()

        rows = self.__requests_elastic.post_search({
            "query": {"ids": {"values": request_numbers}},
            "size": 10000
        }).get('hits', {}).get('hits', []) or []

        result = [self.__restore(row['_source']) for row in rows]

        if len(result) != len(request_numbers):
            message = '{} can\'t find all CancelRequests for Order #{}! Not existed CancelRequests in map: {}'
            raise ValueError(message.format(
                self.get_all_by_order_number,
                order_number.value,
                [
                    request_number for request_number in request_numbers
                    if request_number not in [request.number.value for request in result]
                ]
            ))

        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------


class _CancelRequestStorageDynamoDb(CancelRequestStorageInterface):
    __ENTITY_PROPERTY_REQUEST_NUMBER = '__number'
    __ENTITY_PROPERTY_ORDER_NUMBER = '__order_number'
    __ENTITY_PROPERTY_ITEMS = '__items'
    __ENTITY_PROPERTY_ITEMS_SIMPLE_SKU = '__simple_sku'
    __ENTITY_PROPERTY_ITEMS_QTY = '__qty'
    __ENTITY_PROPERTY_ITEMS_STATUS = '__status'
    __ENTITY_PROPERTY_ITEMS_PROCESSED_AT = '__processed_at'
    __ENTITY_PROPERTY_REFUND_METHOD = '__refund_method'
    __ENTITY_PROPERTY_ADDITIONAL_COMMENT = '__additional_comment'
    __ENTITY_PROPERTY_REQUESTED_AT = '__requested_at'

    def __init__(self):
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_CANCELLATION_REQUEST'
        self.__reflector = Reflector()

    def save(self, cancel_request: CancelRequest) -> None:
        if not isinstance(cancel_request, CancelRequest):
            raise ArgumentTypeException(self.save, 'cancel_request', cancel_request)

        self.__dynamo_db.put_item(cancel_request.number.value, {
            "order_number": cancel_request.order_number.value,
            "requested_at": cancel_request.requested_at.strftime('%Y-%m-%dT%H:%M:%S.%f'),

            "request_items": [{
                "simple_sku": item.simple_sku.value,
                "qty": item.qty.value,
                "status": item.status.value,
                "processed_at": item.processed_at.strftime('%Y-%m-%dT%H:%M:%S.%f') if item.processed_at else None
            } for item in cancel_request.items],

            'refund_method': cancel_request.refund_method.descriptor,
            'refund_method_extra_data_json': json.dumps(cancel_request.refund_method.extra_data),

            "additional_comment": cancel_request.additional_comment.value if cancel_request.additional_comment else None
        })

    def __restore(self, data: dict) -> CancelRequest:
        cancel_request = self.__reflector.construct(CancelRequest, {
            self.__class__.__ENTITY_PROPERTY_REQUEST_NUMBER: CancelRequest.Number(data['sk']),
            self.__class__.__ENTITY_PROPERTY_ORDER_NUMBER: OrderNumber(data['order_number']),
            self.__class__.__ENTITY_PROPERTY_ITEMS: tuple([
                self.__reflector.construct(CancelRequest.Item, {
                    self.__class__.__ENTITY_PROPERTY_ITEMS_SIMPLE_SKU: SimpleSku(item_data['simple_sku']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_QTY: Qty(item_data['qty']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS: CancelRequest.Item.Status(item_data['status']),
                    self.__class__.__ENTITY_PROPERTY_ITEMS_PROCESSED_AT: (
                        datetime.datetime.strptime(item_data['processed_at'], '%Y-%m-%dT%H:%M:%S.%f')
                        if item_data['processed_at'] else None
                    ),
                })
                for item_data in data['request_items']
            ]),
            self.__class__.__ENTITY_PROPERTY_REFUND_METHOD: _restore_refund_method(
                data['refund_method'],
                json.loads(data['refund_method_extra_data_json'])
            ),
            self.__class__.__ENTITY_PROPERTY_ADDITIONAL_COMMENT: (
                CancelRequest.AdditionalComment(data['additional_comment'])
                if data.get('additional_comment') or None
                else None
            ),
            self.__class__.__ENTITY_PROPERTY_REQUESTED_AT: datetime.datetime.strptime(
                data['requested_at'],
                '%Y-%m-%dT%H:%M:%S.%f'
            ),
        })

        return cancel_request

    def get_by_number(self, request_number: CancelRequest.Number) -> Optional[CancelRequest]:
        if not isinstance(request_number, CancelRequest.Number):
            raise ArgumentTypeException(self.get_by_number, 'request_number', request_number)

        data = self.__dynamo_db.find_item(request_number.value)
        return self.__restore(data) if data else None

    def get_all_by_order_number(self, order_number: OrderNumber) -> Tuple[CancelRequest]:
        if not isinstance(order_number, OrderNumber):
            raise ArgumentTypeException(self.get_all_by_order_number, 'order_number', order_number)

        items = self.__dynamo_db.find_by_attribute('order_number', order_number.value)
        result = [self.__restore(item) for item in items]
        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------

