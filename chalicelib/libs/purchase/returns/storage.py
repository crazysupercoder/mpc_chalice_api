import json
from decimal import Decimal
import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.purchase.core import \
    Id, OrderNumber, SimpleSku, Qty, Cost, \
    ReturnRequest, ReturnRequestStorageInterface, \
    RefundMethodAbstract
from chalicelib.libs.purchase.payment_methods.refund_methods import StoreCreditRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import EftRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import CreditCardRefundMethod
from chalicelib.libs.purchase.payment_methods.refund_methods import MobicredRefundMethod


class ReturnRequestStorageImplementation(ReturnRequestStorageInterface):
    def __init__(self):
        self.__storage = _ReturnRequestStorageElastic()

    def save(self, return_request: ReturnRequest) -> None:
        self.__storage.save(return_request)

    def load(self, request_number: ReturnRequest.Number) -> Optional[ReturnRequest]:
        return self.__storage.load(request_number)

    def get_all_for_customer(self, customer_id: Id) -> Tuple[ReturnRequest]:
        return self.__storage.get_all_for_customer(customer_id)


def _restore_delivery_method(descriptor: str) -> ReturnRequest.DeliveryMethod:
    # @todo : refactoring
    from chalicelib.libs.purchase.returns.delivery_methods import HandDeliveryMethod
    from chalicelib.libs.purchase.returns.delivery_methods import CourierOrPostOffice
    from chalicelib.libs.purchase.returns.delivery_methods import RunwaysaleToCollect
    methods_map = {
        'hand_delivery': HandDeliveryMethod,
        'courier_or_post_office': CourierOrPostOffice,
        'runwaysale_to_collect': RunwaysaleToCollect,
    }
    method_class = methods_map.get(descriptor)
    if not method_class:
        raise ValueError('{} does not know, how to work with {} delivery method!'.format(
            _restore_delivery_method,
            descriptor
        ))

    return method_class()


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


class _ReturnRequestStorageElastic(ReturnRequestStorageInterface):
    """
        curl -X DELETE localhost:9200/purchase_return_requests
        curl -X PUT localhost:9200/purchase_return_requests -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_return_requests": {
                    "properties": {
                        "request_number": {"type": "keyword"},
                        "customer_id": {"type": "keyword"},
                        "request_items": {
                            "properties": {
                                "order_number": {"type": "keyword"},
                                "simple_sku": {"type": "keyword"},
                                "qty": {"type": "integer"},
                                "cost": {"type": "float"},
                                "reason": {"type": "keyword"},
                                "additional_comment": {"type": "keyword"},
                                "attached_files_urls_json": {"type": "keyword"},
                                "status_history": {
                                    "properties": {
                                        "status": {"type": "keyword"},
                                        "datetime": {"type": "date", "format": "date_hour_minute_second_millis"}
                                    }
                                }
                            }
                        },
                        "delivery_method": {"type": "keyword"},
                        "refund_method": {"type": "keyword"},
                        "refund_method_extra_data_json": {"type": "keyword"}
                    }
                }
            }
        }'

        curl -X DELETE localhost:9200/purchase_return_requests_customer_map
        curl -X PUT localhost:9200/purchase_return_requests_customer_map -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_return_requests_customer_map": {
                    "properties": {
                        "request_numbers_json": {"type": "keyword"}
                    }
                }
            }
        }'
    """

    __ENTITY_PROPERTY_REQUEST_NUMBER = '__number'
    __ENTITY_PROPERTY_CUSTOMER_ID = '__customer_id'
    __ENTITY_PROPERTY_REFUND_METHOD = '__refund_method'
    __ENTITY_PROPERTY_DELIVERY_METHOD = '__delivery_method'
    __ENTITY_PROPERTY_ITEMS = '__items'
    __ENTITY_PROPERTY_ITEMS_ORDER_NUMBER = '__order_number'
    __ENTITY_PROPERTY_ITEMS_SIMPLE_SKU = '__simple_sku'
    __ENTITY_PROPERTY_ITEMS_QTY = '__qty'
    __ENTITY_PROPERTY_ITEMS_COST = '__cost'
    __ENTITY_PROPERTY_ITEMS_REASON = '__reason'
    __ENTITY_PROPERTY_ITEMS_ATTACHED_FILES = '__attached_files'
    __ENTITY_PROPERTY_ITEMS_ADDITIONAL_COMMENT = '__additional_comment'
    __ENTITY_PROPERTY_ITEMS_STATUS_HISTORY = '__status_history'

    def __init__(self):
        self.__requests_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS,
            settings.AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS
        )
        self.__customer_requests_map_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS_CUSTOMER_MAP,
            settings.AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS_CUSTOMER_MAP
        )
        self.__reflector = Reflector()

    def save(self, return_request: ReturnRequest) -> None:
        if not isinstance(return_request, ReturnRequest):
            raise ArgumentTypeException(self.save, 'return_request', return_request)

        items_data = []
        for item in return_request.items:
            status_history: ReturnRequest.Item.StatusChangesHistory = self.__reflector.extract(item, (
                self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY,
            ))[self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY]

            items_data.append({
                "order_number": item.order_number.value,
                "simple_sku": item.simple_sku.value,
                "qty": item.qty.value,
                "cost": item.cost.value,
                "reason": item.reason.descriptor,
                "additional_comment": item.additional_comment.value if item.additional_comment else None,
                "attached_files_urls_json": json.dumps([file.url for file in item.attached_files]),
                "status_history": [{
                    'status': status_change.status.value,
                    # elastic supports only 3 digits for milliseconds
                    'datetime': status_change.datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                } for status_change in status_history.get_all()]
            })

        document_id = return_request.number.value
        document_data = {
            "request_number": return_request.number.value,
            "customer_id": return_request.customer_id.value,
            "request_items": items_data,
            "delivery_method": return_request.delivery_method.descriptor,
            "refund_method": return_request.refund_method.descriptor,
            "refund_method_extra_data_json": json.dumps(return_request.refund_method.extra_data),
        }

        existed_request = self.load(return_request.number)
        if existed_request:
            # just a double check of number uniqueness
            if existed_request.customer_id != return_request.customer_id:
                raise RuntimeError(
                    'Return Request "{}" already exists and belongs to another Customer!'.format(return_request.number)
                )

            self.__requests_elastic.update_data(document_id, {
                'doc': document_data
            })
        else:
            self.__requests_elastic.create(document_id, document_data)

            # Elastic can search by attributes only after 1 second from last update.
            # We need all data, when we are searching by customer_id,
            # so in this case we will lost fresh data, if search directly after creation of a new return request.
            # In this case we need to use another index and get data by elastic doc_id.

            customer_requests_map = self.__customer_requests_map_elastic.get_data(return_request.customer_id.value)
            if customer_requests_map:
                request_numbers = list(json.loads(customer_requests_map.get('request_numbers_json', '[]')) or [])
                request_numbers.append(return_request.number.value)
                request_numbers = list(set(request_numbers))
                self.__customer_requests_map_elastic.update_data(return_request.customer_id.value, {
                    'doc': {
                        'request_numbers_json': json.dumps(request_numbers)
                    }
                })
            else:
                self.__customer_requests_map_elastic.create(return_request.customer_id.value, {
                    'request_numbers_json': json.dumps([return_request.number.value])
                })

    def load(self, request_number: ReturnRequest.Number) -> Optional[ReturnRequest]:
        if not isinstance(request_number, ReturnRequest.Number):
            raise ArgumentTypeException(self.load, 'request_number', request_number)

        data = self.__requests_elastic.get_data(request_number.value)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data: dict) -> ReturnRequest:
        request_items = []
        for item_data in data['request_items']:
            attached_files = json.loads(item_data['attached_files_urls_json'])
            attached_files = tuple([ReturnRequest.Item.AttachedFile(url) for url in attached_files])

            additional_comment = ReturnRequest.Item.AdditionalComment(item_data['additional_comment'])

            status_history = ReturnRequest.Item.StatusChangesHistory(tuple([
                self.__reflector.construct(ReturnRequest.Item.StatusChangesHistory.Change, {
                    '__status': ReturnRequest.Item.Status(change['status']),
                    # elastic supports only 3 digits for milliseconds
                    '__datetime': datetime.datetime.strptime(change['datetime'] + '000', '%Y-%m-%dT%H:%M:%S.%f'),
                }) for change in item_data['status_history']
            ]))

            request_items.append(self.__reflector.construct(ReturnRequest.Item, {
                self.__class__.__ENTITY_PROPERTY_ITEMS_ORDER_NUMBER: OrderNumber(item_data['order_number']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_SIMPLE_SKU: SimpleSku(item_data['simple_sku']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_QTY: Qty(item_data['qty']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_COST: Cost(item_data['cost']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_REASON: ReturnRequest.Item.Reason(item_data['reason']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_ATTACHED_FILES: attached_files,
                self.__class__.__ENTITY_PROPERTY_ITEMS_ADDITIONAL_COMMENT: additional_comment,
                self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY: status_history,
            }))

        return_request = self.__reflector.construct(ReturnRequest, {
            self.__class__.__ENTITY_PROPERTY_REQUEST_NUMBER: ReturnRequest.Number(data['request_number']),
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID: Id(data['customer_id']),
            self.__class__.__ENTITY_PROPERTY_ITEMS: tuple(request_items),
            self.__class__.__ENTITY_PROPERTY_DELIVERY_METHOD: _restore_delivery_method(data['delivery_method']),
            self.__class__.__ENTITY_PROPERTY_REFUND_METHOD: _restore_refund_method(
                data['refund_method'],
                json.loads(data['refund_method_extra_data_json'])
            ),
        })

        return return_request

    def get_all_for_customer(self, customer_id: Id) -> Tuple[ReturnRequest]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.get_all_for_customer, 'customer_id', customer_id)

        data = self.__customer_requests_map_elastic.get_data(customer_id.value)
        request_numbers = json.loads((data.get('request_numbers_json') or '[]') if data else '[]') or []
        if not request_numbers:
            return tuple()

        rows = self.__requests_elastic.post_search({
            "query": {"ids": {"values": request_numbers}},
            "size": 10000
        }).get('hits', {}).get('hits', []) or []

        result = [self.__restore(row['_source']) for row in rows]

        if len(result) != len(request_numbers):
            message = '{} can\'t find all Return-Requests for Customer #{}! Not existed Return-Requests in map: {}'
            raise ValueError(message.format(
                self.get_all_for_customer,
                customer_id.value,
                [
                    request_number for request_number in request_numbers
                    if request_number not in [request.number.value for request in result]
                ]
            ))

        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------


class _ReturnRequestStorageDynamoDb(ReturnRequestStorageInterface):
    __ENTITY_PROPERTY_REQUEST_NUMBER = '__number'
    __ENTITY_PROPERTY_CUSTOMER_ID = '__customer_id'
    __ENTITY_PROPERTY_REFUND_METHOD = '__refund_method'
    __ENTITY_PROPERTY_DELIVERY_METHOD = '__delivery_method'
    __ENTITY_PROPERTY_ITEMS = '__items'
    __ENTITY_PROPERTY_ITEMS_ORDER_NUMBER = '__order_number'
    __ENTITY_PROPERTY_ITEMS_SIMPLE_SKU = '__simple_sku'
    __ENTITY_PROPERTY_ITEMS_QTY = '__qty'
    __ENTITY_PROPERTY_ITEMS_COST = '__cost'
    __ENTITY_PROPERTY_ITEMS_REASON = '__reason'
    __ENTITY_PROPERTY_ITEMS_ATTACHED_FILES = '__attached_files'
    __ENTITY_PROPERTY_ITEMS_ADDITIONAL_COMMENT = '__additional_comment'
    __ENTITY_PROPERTY_ITEMS_STATUS_HISTORY = '__status_history'

    def __init__(self):
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_RETURN_REQUESTS'

        self.__reflector = Reflector()

    def save(self, return_request: ReturnRequest) -> None:
        if not isinstance(return_request, ReturnRequest):
            raise ArgumentTypeException(self.save, 'return_request', return_request)

        items_data = []
        for item in return_request.items:
            status_history: ReturnRequest.Item.StatusChangesHistory = self.__reflector.extract(item, (
                self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY,
            ))[self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY]

            items_data.append({
                'order_number': item.order_number.value,
                'simple_sku': item.simple_sku.value,
                'qty': item.qty.value,
                'cost': item.cost.value,
                'reason': item.reason.descriptor,
                'additional_comment': item.additional_comment.value if item.additional_comment else None,
                'attached_files_urls_json': json.dumps([file.url for file in item.attached_files]),
                'status_history': [{
                    'status': status_change.status.value,
                    'datetime': status_change.datetime.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                } for status_change in status_history.get_all()]
            })

        document_id = return_request.number.value
        document_data = {
            'customer_id': return_request.customer_id.value,
            'request_items': items_data,
            'delivery_method': return_request.delivery_method.descriptor,
            'refund_method': return_request.refund_method.descriptor,
            'refund_method_extra_data_json': json.dumps(return_request.refund_method.extra_data),
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        document_data = json.loads(json.dumps(document_data), parse_float=Decimal)

        self.__dynamo_db.put_item(document_id, document_data)

    def load(self, request_number: ReturnRequest.Number) -> Optional[ReturnRequest]:
        if not isinstance(request_number, ReturnRequest.Number):
            raise ArgumentTypeException(self.load, 'request_number', request_number)

        data = self.__dynamo_db.find_item(request_number.value)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data: dict) -> ReturnRequest:
        request_items = []
        for item_data in data['request_items']:
            attached_files = json.loads(item_data['attached_files_urls_json'])
            attached_files = tuple([ReturnRequest.Item.AttachedFile(url) for url in attached_files])

            additional_comment = ReturnRequest.Item.AdditionalComment(item_data['additional_comment'])

            status_history = ReturnRequest.Item.StatusChangesHistory(tuple([
                self.__reflector.construct(ReturnRequest.Item.StatusChangesHistory.Change, {
                    '__status': ReturnRequest.Item.Status(change['status']),
                    '__datetime': datetime.datetime.strptime(change['datetime'], '%Y-%m-%dT%H:%M:%S.%f'),
                }) for change in item_data['status_history']
            ]))

            request_items.append(self.__reflector.construct(ReturnRequest.Item, {
                self.__class__.__ENTITY_PROPERTY_ITEMS_ORDER_NUMBER: OrderNumber(item_data['order_number']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_SIMPLE_SKU: SimpleSku(item_data['simple_sku']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_QTY: Qty(item_data['qty']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_COST: Cost(item_data['cost']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_REASON: ReturnRequest.Item.Reason(item_data['reason']),
                self.__class__.__ENTITY_PROPERTY_ITEMS_ATTACHED_FILES: attached_files,
                self.__class__.__ENTITY_PROPERTY_ITEMS_ADDITIONAL_COMMENT: additional_comment,
                self.__class__.__ENTITY_PROPERTY_ITEMS_STATUS_HISTORY: status_history,
            }))

        return_request = self.__reflector.construct(ReturnRequest, {
            self.__class__.__ENTITY_PROPERTY_REQUEST_NUMBER: ReturnRequest.Number(data['request_number']),
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID: Id(data['customer_id']),
            self.__class__.__ENTITY_PROPERTY_ITEMS: tuple(request_items),
            self.__class__.__ENTITY_PROPERTY_DELIVERY_METHOD: _restore_delivery_method(data['delivery_method']),
            self.__class__.__ENTITY_PROPERTY_REFUND_METHOD: _restore_refund_method(
                data['refund_method'],
                json.loads(data['refund_method_extra_data_json'])
            ),
        })

        return return_request

    def get_all_for_customer(self, customer_id: Id) -> Tuple[ReturnRequest]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.get_all_for_customer, 'customer_id', customer_id)

        items = self.__dynamo_db.find_by_attribute('customer_id', customer_id.value)
        result = [self.__restore(item) for item in items]
        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------

