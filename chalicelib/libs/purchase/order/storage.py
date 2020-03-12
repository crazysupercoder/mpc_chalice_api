import json
import datetime
from decimal import Decimal
from typing import Optional, Tuple
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.purchase.core import \
    Id, EventCode, SimpleSku, Qty, Cost, DeliveryAddress, \
    Name, Description, Percentage, \
    Dtd, Order, OrderStorageInterface
from chalicelib.libs.purchase.payment_methods.regular_eft.payment import RegularEftOrderPaymentMethod
from chalicelib.libs.purchase.payment_methods.customer_credits import CustomerCreditsOrderPaymentMethod
from chalicelib.libs.purchase.payment_methods.peach.payments import MobicredPaymentMethod, CreditCardOrderPaymentMethod
from chalicelib.libs.purchase.settings import PurchaseSettings


# ----------------------------------------------------------------------------------------------------------------------


class _OrderElasticStorage(OrderStorageInterface):
    """
        curl -X DELETE localhost:9200/purchase_orders
        curl -X PUT localhost:9200/purchase_orders -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_orders": {
                    "properties": {
                        "order_number": {"type": "keyword"},
                        "customer_id": {"type": "keyword"},
                        "order_items": {
                            "properties": {
                                "event_code": {"type": "keyword"},
                                "simple_sku": {"type": "keyword"},
                                "product_original_price": {"type": "float"},
                                "product_current_price": {"type": "float"},
                                "dtd_occasion_name": {"type": "keyword"},
                                "dtd_occasion_description": {"type": "keyword"},
                                "dtd_date_from": {"type": "date", "format": "date"},
                                "dtd_date_to": {"type": "date", "format": "date"},
                                "dtd_min": {"type": "integer"},
                                "dtd_max":  {"type": "integer"},
                                "qty_ordered": {"type": "integer"},
                                "qty_return_requested": {"type": "integer"},
                                "qty_return_returned": {"type": "integer"},
                                "qty_cancelled_before_payment": {"type": "integer"},
                                "qty_cancelled_after_payment_requested": {"type": "integer"},
                                "qty_cancelled_after_payment_cancelled": {"type": "integer"},
                                "qty_refunded":  {"type": "integer"},
                                "qty_modified_at": {"type": "date", "format": "date_hour_minute_second_millis"},
                                "fbucks_amount": {"type": "float"}
                            }
                        },
                        "delivery_address_recipient_name": {"type": "keyword"},
                        "delivery_address_phone_number": {"type": "keyword"},
                        "delivery_address_street_address": {"type": "keyword"},
                        "delivery_address_suburb": {"type": "keyword"},
                        "delivery_address_city": {"type": "keyword"},
                        "delivery_address_province": {"type": "keyword"},
                        "delivery_address_complex_building": {"type": "keyword"},
                        "delivery_address_postal_code": {"type": "keyword"},
                        "delivery_address_business_name": {"type": "keyword"},
                        "delivery_address_special_instructions": {"type": "keyword"},
                        "delivery_cost": {"type": "float"},
                        "vat_percent": {"type": "float"},
                        "credits_spent": {"type": "float"},
                        "payment_method": {"type": "keyword"},
                        "payment_method_extra_data_json": {"type": "keyword"},
                        "status_history": {
                            "properties": {
                                "status": {"type": "keyword"},
                                "datetime": {"type": "date", "format": "date_hour_minute_second_millis"}
                            }
                        }
                    }
                }
            }
        }'

        curl -X DELETE localhost:9200/purchase_orders_customer_orders_map
        curl -X PUT localhost:9200/purchase_orders_customer_orders_map -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_orders_customer_orders_map": {
                    "properties": {
                        "order_numbers_json": {"type": "keyword"}
                    }
                }
            }
        }'
    """

    def __init__(self):
        self.__orders_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_ORDERS,
            settings.AWS_ELASTICSEARCH_PURCHASE_ORDERS
        )
        self.__customer_orders_map_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_ORDERS_CUSTOMER_ORDERS_MAP,
            settings.AWS_ELASTICSEARCH_PURCHASE_ORDERS_CUSTOMER_ORDERS_MAP
        )
        self.__reflector = Reflector()
        self.__current_vat_value = PurchaseSettings().vat

    def save(self, order: Order) -> None:
        if not isinstance(order, Order):
            raise ArgumentTypeException(self.save, 'order', order)

        order_number = order.number
        delivery_address = order.delivery_address
        status_changes = order.status_history

        document_id = order_number.value
        document_data = {
            'order_number': order_number.value,
            'customer_id': order.customer_id.value,
            'order_items': [{
                'event_code': item.event_code.value,
                'simple_sku': item.simple_sku.value,
                'product_original_price': item.product_original_price.value,
                'product_current_price': item.product_current_price.value,
                'dtd_occasion_name': item.dtd.occasion.name.value if item.dtd.occasion else None,
                'dtd_occasion_description': item.dtd.occasion.description.value if item.dtd.occasion else None,
                'dtd_date_from': item.dtd.date_from.strftime('%Y-%m-%d'),
                'dtd_date_to': item.dtd.date_to.strftime('%Y-%m-%d'),
                'dtd_working_days_from': item.dtd.working_days_from,
                'dtd_working_days_to': item.dtd.working_days_to,
                'qty_ordered': item.qty_ordered.value,
                'qty_return_requested': item.qty_return_requested.value,
                'qty_return_returned': item.qty_return_returned.value,
                'qty_cancelled_before_payment': item.qty_cancelled_before_payment.value,
                'qty_cancelled_after_payment_requested': item.qty_cancelled_after_payment_requested.value,
                'qty_cancelled_after_payment_cancelled': item.qty_cancelled_after_payment_cancelled.value,
                'qty_refunded': item.qty_refunded.value,

                # elastic supports only 3 digits for milliseconds
                'qty_modified_at': item.qty_modified_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],

                'fbucks_amount': item.fbucks_earnings.value,
            } for item in order.items],
            'delivery_address_recipient_name': delivery_address.recipient_name,
            'delivery_address_phone_number': delivery_address.phone_number,
            'delivery_address_street_address': delivery_address.street_address,
            'delivery_address_suburb': delivery_address.suburb,
            'delivery_address_city': delivery_address.city,
            'delivery_address_province': delivery_address.province,
            'delivery_address_complex_building': delivery_address.complex_building,
            'delivery_address_postal_code': delivery_address.postal_code,
            'delivery_address_business_name': delivery_address.business_name,
            'delivery_address_special_instructions': delivery_address.special_instructions,
            'delivery_cost': order.delivery_cost.value,
            'vat_percent': order.vat_percent.value,
            'credits_spent': order.credit_spent_amount.value,
            'payment_method': order.payment_method.descriptor if order.payment_method else None,
            'payment_method_extra_data_json': json.dumps(
                order.payment_method.extra_data if order.payment_method else {}
            ),
            'status_history': [{
                'status': status_change.status.value,

                # elastic supports only 3 digits for milliseconds
                'datetime': status_change.datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],

            } for status_change in status_changes],
        }

        existed_order = self.load(order_number)
        if existed_order:
            # just a double check of order number uniqueness
            if existed_order.customer_id != order.customer_id:
                raise RuntimeError(
                    'Order "{}" already exists and belongs to another Customer!'.format(order_number))

            self.__orders_elastic.update_data(document_id, {
                'doc': document_data
            })
        else:
            self.__orders_elastic.create(document_id, document_data)

            # Elastic can search by attributes only after 1 second from last update.
            # We need all data, when we are searching by customer_id,
            # so in this case we will lost fresh data, if search directly after creation of new order.
            # In this case we need to use another index and get data by elastic doc_id.

            customer_orders_map = self.__customer_orders_map_elastic.get_data(order.customer_id.value)
            if customer_orders_map:
                order_numbers = list(json.loads(customer_orders_map.get('order_numbers_json', '[]')) or [])
                order_numbers.append(order.number.value)
                order_numbers = list(set(order_numbers))
                self.__customer_orders_map_elastic.update_data(order.customer_id.value, {
                    'doc': {
                        'order_numbers_json': json.dumps(order_numbers)
                    }
                })
            else:
                self.__customer_orders_map_elastic.create(order.customer_id.value, {
                    'order_numbers_json': json.dumps([order.number.value])
                })

    def load(self, order_number: Order.Number) -> Optional[Order]:
        if not isinstance(order_number, Order.Number):
            raise ArgumentTypeException(self.load, 'order_number', order_number)

        data = self.__orders_elastic.get_data(order_number.value)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data: dict) -> Order:
        order_number = Order.Number(data.get('order_number'))
        customer_id = Id(data.get('customer_id'))
        delivery_cost = Cost(float(data.get('delivery_cost')))
        vat_percent = Percentage(float(
            # I added "vat_percent" after first orders were stored,
            # but it's hard to make changes in elastic, so...
            # @todo : create migration tool.
            data.get('vat_percent') or self.__current_vat_value
        ))
        credits_spent = Cost(float(data.get('credits_spent') or '0'))  # can be not existed in old data
        payment_method = self.__restore_payment_method(
            data.get('payment_method'),
            json.loads(data.get('payment_method_extra_data_json') or '{}') if data.get('payment_method') else None
        )

        delivery_address = DeliveryAddress(
            data.get('delivery_address_recipient_name'),
            data.get('delivery_address_phone_number'),
            data.get('delivery_address_street_address'),
            data.get('delivery_address_suburb'),
            data.get('delivery_address_city'),
            data.get('delivery_address_province'),
            data.get('delivery_address_complex_building'),
            data.get('delivery_address_postal_code'),
            data.get('delivery_address_business_name'),
            data.get('delivery_address_special_instructions')
        )

        status_changes = []
        for status_change_data in data.get('status_history'):
            status = Order.Status(status_change_data.get('status'))

            # elastic supports only 3 digits for milliseconds
            changed_at = datetime.datetime.strptime(status_change_data.get('datetime') + '000', '%Y-%m-%dT%H:%M:%S.%f')

            status_change = self.__reflector.construct(Order.StatusChangesHistory.Change, {
                '__status': status,
                '__datetime': changed_at
            })
            status_changes.append(status_change)

        status_change_history = Order.StatusChangesHistory(tuple(status_changes))

        order_items = []
        for item_data in data.get('order_items'):
            event_code = EventCode(item_data.get('event_code'))
            simple_sku = SimpleSku(item_data.get('simple_sku'))
            product_original_price = Cost(item_data.get('product_original_price'))
            product_current_price = Cost(item_data.get('product_current_price'))
            fbucks_earnings = Cost(item_data.get('fbucks_amount') or 0)  # old orders don't have this field
            dtd = Dtd(
                Dtd.Occasion(
                    Name(item_data.get('dtd_occasion_name')),
                    Description(item_data.get('dtd_occasion_description'))
                ) if item_data.get('dtd_occasion_name') else None,
                datetime.date(
                    int(item_data.get('dtd_date_from').split('-')[0]),
                    int(item_data.get('dtd_date_from').split('-')[1]),
                    int(item_data.get('dtd_date_from').split('-')[2])
                ),
                datetime.date(
                    int(item_data.get('dtd_date_to').split('-')[0]),
                    int(item_data.get('dtd_date_to').split('-')[1]),
                    int(item_data.get('dtd_date_to').split('-')[2])
                ),
                int(item_data.get('dtd_working_days_from')),
                int(item_data.get('dtd_working_days_to'))
            )
            qty_ordered = Qty(int(item_data.get('qty_ordered')))
            qty_return_requested = Qty(int(item_data.get('qty_return_requested') or 0))
            qty_return_returned = Qty(int(item_data.get('qty_return_returned') or 0))
            qty_cancelled_before_payment = Qty(int(item_data.get('qty_cancelled_before_payment') or 0))
            qty_cancelled_after_payment_requested = Qty(int(item_data.get('qty_cancelled_after_payment_requested') or 0))
            qty_cancelled_after_payment_cancelled = Qty(int(item_data.get('qty_cancelled_after_payment_cancelled') or 0))
            qty_refunded = Qty(int(item_data.get('qty_refunded') or 0))

            # elastic supports only 3 digits for milliseconds
            qty_modified_at = datetime.datetime.strptime((
                # "qty_modified_at" may not exist for old data (dev, test),
                # but it's hard to make changes in elastic, so...
                # @todo : create migration tool.
                item_data.get('qty_modified_at')
                or status_change_history.get_last().datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
            ) + '000', '%Y-%m-%dT%H:%M:%S.%f')

            order_item = self.__reflector.construct(Order.Item, {
                '__event_code': event_code,
                '__simple_sku': simple_sku,
                '__product_original_price': product_original_price,
                '__product_current_price': product_current_price,
                '__dtd': dtd,
                '__qty_ordered': qty_ordered,
                '__qty_return_requested': qty_return_requested,
                '__qty_return_returned': qty_return_returned,
                '__qty_cancelled_before_payment': qty_cancelled_before_payment,
                '__qty_cancelled_after_payment_requested': qty_cancelled_after_payment_requested,
                '__qty_cancelled_after_payment_cancelled': qty_cancelled_after_payment_cancelled,
                '__qty_refunded': qty_refunded,
                '__qty_modified_at': qty_modified_at,
                '__fbucks_earnings': fbucks_earnings
            })
            order_items.append(order_item)

        order = self.__reflector.construct(Order, {
            '__order_number': order_number,
            '__customer_id': customer_id,
            '__items': order_items,
            '__delivery_address': delivery_address,
            '__delivery_cost': delivery_cost,
            '__vat_percent': vat_percent,
            '__payment_method': payment_method,
            '__status_history': status_change_history,
            '__credits_spent': credits_spent,
        })

        return order

    def __restore_payment_method(
        self,
        descriptor: Optional[str],
        extra_data: Optional[dict],
    ) -> Optional[Order.PaymentMethodAbstract]:
        if not descriptor:
            return None

        # @todo : refactoring !!!

        if descriptor == 'regular_eft':
            return RegularEftOrderPaymentMethod()
        elif descriptor == 'mobicred':
            return MobicredPaymentMethod(extra_data['payment_id'])
        elif descriptor == 'credit_card':
            return CreditCardOrderPaymentMethod(extra_data['payment_id'])
        elif descriptor == 'customer_credit':
            return CustomerCreditsOrderPaymentMethod()

        raise Exception('{} does not know, how to restore {} payment method with data {}!'.format(
            self.__restore_payment_method,
            descriptor,
            extra_data
        ))

    def get_all_by_numbers(self, order_numbers: Tuple[Order.Number]) -> Tuple[Order]:
        if sum([not isinstance(order_number, Order.Number) for order_number in order_numbers]) > 0:
            raise ArgumentTypeException(self.get_all_by_numbers, 'order_numbers', order_numbers)

        rows = self.__orders_elastic.post_search({
            "query": {"ids": {"values": [order_number.value for order_number in order_numbers]}},
            "size": 10000
        }).get('hits', {}).get('hits', []) or []

        result = [self.__restore(row['_source']) for row in rows]
        return tuple(result)

    def get_all_for_customer(self, customer_id: Id) -> Tuple[Order]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.get_all_for_customer, 'customer_id', customer_id)

        data = self.__customer_orders_map_elastic.get_data(customer_id.value)
        order_numbers = json.loads((data.get('order_numbers_json') or '[]') if data else '[]') or []
        if not order_numbers:
            return tuple()

        rows = self.__orders_elastic.post_search({
            "query": {"ids": {"values": order_numbers}},
            "size": 10000
        }).get('hits', {}).get('hits', []) or []

        result = [self.__restore(row['_source']) for row in rows]

        if len(result) != len(order_numbers):
            message = '{} can\'t find all Orders for Customer #{}! Not existed order in map: {}'
            raise ValueError(message.format(
                self.get_all_for_customer,
                customer_id.value,
                [
                    order_number for order_number in order_numbers
                    if order_number not in [order.number.value for order in result]
                ]
            ))

        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------


class _OrderStorageDynamoDb(OrderStorageInterface):
    def __init__(self):
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_ORDERS'
        self.__reflector = Reflector()

    def save(self, order: Order) -> None:
        if not isinstance(order, Order):
            raise ArgumentTypeException(self.save, 'order', order)

        order_number = order.number
        delivery_address = order.delivery_address
        status_changes = order.status_history

        document_id = order_number.value
        document_data = {
            'customer_id': order.customer_id.value,
            'order_items': [{
                'event_code': item.event_code.value,
                'simple_sku': item.simple_sku.value,
                'product_original_price': item.product_original_price.value,
                'product_current_price': item.product_current_price.value,
                'dtd_occasion_name': item.dtd.occasion.name.value if item.dtd.occasion else None,
                'dtd_occasion_description': item.dtd.occasion.description.value if item.dtd.occasion else None,
                'dtd_date_from': item.dtd.date_from.strftime('%Y-%m-%d'),
                'dtd_date_to': item.dtd.date_to.strftime('%Y-%m-%d'),
                'dtd_working_days_from': item.dtd.working_days_from,
                'dtd_working_days_to': item.dtd.working_days_to,
                'qty_ordered': item.qty_ordered.value,
                'qty_cancelled_before_payment': item.qty_cancelled_before_payment.value,
                'qty_cancelled_after_payment_requested': item.qty_cancelled_after_payment_requested.value,
                'qty_cancelled_after_payment_cancelled': item.qty_cancelled_after_payment_cancelled.value,
                'qty_return_requested': item.qty_return_requested.value,
                'qty_return_returned': item.qty_return_returned.value,
                'qty_refunded': item.qty_refunded.value,
                'qty_modified_at': item.qty_modified_at.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                'fbucks_earnings': item.fbucks_earnings.value,
            } for item in order.items],
            'delivery_address_recipient_name': delivery_address.recipient_name,
            'delivery_address_phone_number': delivery_address.phone_number,
            'delivery_address_street_address': delivery_address.street_address,
            'delivery_address_suburb': delivery_address.suburb,
            'delivery_address_city': delivery_address.city,
            'delivery_address_province': delivery_address.province,
            'delivery_address_complex_building': delivery_address.complex_building,
            'delivery_address_postal_code': delivery_address.postal_code,
            'delivery_address_business_name': delivery_address.business_name,
            'delivery_address_special_instructions': delivery_address.special_instructions,
            'delivery_cost': order.delivery_cost.value,
            'vat_percent': order.vat_percent.value,
            'credits_spent': order.credit_spent_amount.value,
            'payment_method': order.payment_method.descriptor if order.payment_method else None,
            'payment_method_extra_data_json': json.dumps(
                order.payment_method.extra_data if order.payment_method else {}
            ),
            'status_history': [{
                'status': status_change.status.value,
                'datetime': status_change.datetime.strftime('%Y-%m-%dT%H:%M:%S.%f'),

            } for status_change in status_changes],
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        document_data = json.loads(json.dumps(document_data), parse_float=Decimal)

        self.__dynamo_db.put_item(document_id, document_data)

    def __restore(self, data: dict) -> Order:
        order_number = Order.Number(data.get('sk'))
        customer_id = Id(data.get('customer_id'))
        delivery_cost = Cost(float(data.get('delivery_cost')))
        vat_percent = Percentage(float(data.get('vat_percent')))
        credits_spent = Cost(float(data.get('credits_spent') or '0'))

        payment_method = self.__restore_payment_method(
            data.get('payment_method'),
            json.loads(data.get('payment_method_extra_data_json') or '{}') if data.get('payment_method') else None
        )

        delivery_address = DeliveryAddress(
            data.get('delivery_address_recipient_name'),
            data.get('delivery_address_phone_number'),
            data.get('delivery_address_street_address'),
            data.get('delivery_address_suburb'),
            data.get('delivery_address_city'),
            data.get('delivery_address_province'),
            data.get('delivery_address_complex_building'),
            data.get('delivery_address_postal_code'),
            data.get('delivery_address_business_name'),
            data.get('delivery_address_special_instructions')
        )

        status_changes = []
        for status_change_data in data.get('status_history'):
            status = Order.Status(status_change_data.get('status'))
            changed_at = datetime.datetime.strptime(status_change_data.get('datetime'), '%Y-%m-%dT%H:%M:%S.%f')
            status_change = self.__reflector.construct(Order.StatusChangesHistory.Change, {
                '__status': status,
                '__datetime': changed_at
            })
            status_changes.append(status_change)

        status_change_history = Order.StatusChangesHistory(tuple(status_changes))

        order_items = []
        for item_data in data.get('order_items'):
            event_code = EventCode(item_data.get('event_code'))
            simple_sku = SimpleSku(item_data.get('simple_sku'))
            product_original_price = Cost(item_data.get('product_original_price'))
            product_current_price = Cost(item_data.get('product_current_price'))
            fbucks_earnings = Cost(item_data.get('fbucks_earnings'))
            dtd = Dtd(
                Dtd.Occasion(
                    Name(item_data.get('dtd_occasion_name')),
                    Description(item_data.get('dtd_occasion_description'))
                ) if item_data.get('dtd_occasion_name') else None,
                datetime.date(
                    int(item_data.get('dtd_date_from').split('-')[0]),
                    int(item_data.get('dtd_date_from').split('-')[1]),
                    int(item_data.get('dtd_date_from').split('-')[2])
                ),
                datetime.date(
                    int(item_data.get('dtd_date_to').split('-')[0]),
                    int(item_data.get('dtd_date_to').split('-')[1]),
                    int(item_data.get('dtd_date_to').split('-')[2])
                ),
                int(item_data.get('dtd_working_days_from')),
                int(item_data.get('dtd_working_days_to'))
            )

            qty_ordered = Qty(int(item_data.get('qty_ordered')))
            qty_return_requested = Qty(int(item_data.get('qty_return_requested') or 0))
            qty_return_returned = Qty(int(item_data.get('qty_return_returned') or 0))
            qty_cancelled_before_payment = Qty(int(item_data.get('qty_cancelled_before_payment') or 0))
            qty_cancelled_after_payment_requested = Qty(int(item_data.get('qty_cancelled_after_payment_requested') or 0))
            qty_cancelled_after_payment_cancelled = Qty(int(item_data.get('qty_cancelled_after_payment_cancelled') or 0))
            qty_refunded = Qty(int(item_data.get('qty_refunded') or 0))
            qty_modified_at = datetime.datetime.strptime(item_data.get('qty_modified_at'), '%Y-%m-%dT%H:%M:%S.%f')

            order_item = self.__reflector.construct(Order.Item, {
                '__event_code': event_code,
                '__simple_sku': simple_sku,
                '__product_original_price': product_original_price,
                '__product_current_price': product_current_price,
                '__dtd': dtd,
                '__qty_ordered': qty_ordered,
                '__qty_return_requested': qty_return_requested,
                '__qty_return_returned': qty_return_returned,
                '__qty_cancelled_before_payment': qty_cancelled_before_payment,
                '__qty_cancelled_after_payment_requested': qty_cancelled_after_payment_requested,
                '__qty_cancelled_after_payment_cancelled': qty_cancelled_after_payment_cancelled,
                '__qty_refunded': qty_refunded,
                '__qty_modified_at': qty_modified_at,
                '__fbucks_earnings': fbucks_earnings
            })
            order_items.append(order_item)

        order = self.__reflector.construct(Order, {
            '__order_number': order_number,
            '__customer_id': customer_id,
            '__items': order_items,
            '__delivery_address': delivery_address,
            '__delivery_cost': delivery_cost,
            '__vat_percent': vat_percent,
            '__payment_method': payment_method,
            '__status_history': status_change_history,
            '__credits_spent': credits_spent,
        })

        return order

    def __restore_payment_method(
        self,
        descriptor: Optional[str],
        extra_data: Optional[dict],
    ) -> Optional[Order.PaymentMethodAbstract]:
        if not descriptor:
            return None

        # @todo : refactoring !!!

        if descriptor == 'regular_eft':
            return RegularEftOrderPaymentMethod()
        elif descriptor == 'mobicred':
            return MobicredPaymentMethod(extra_data['payment_id'])
        elif descriptor == 'credit_card':
            return CreditCardOrderPaymentMethod(extra_data['payment_id'])
        elif descriptor == 'customer_credit':
            return CustomerCreditsOrderPaymentMethod()

        raise Exception('{} does not know, how to restore {} payment method with data {}!'.format(
            self.__restore_payment_method,
            descriptor,
            extra_data
        ))

    def load(self, order_number: Order.Number) -> Optional[Order]:
        if not isinstance(order_number, Order.Number):
            raise ArgumentTypeException(self.load, 'order_number', order_number)

        data = self.__dynamo_db.find_item(order_number.value)
        return self.__restore(data) if data else None

    def get_all_by_numbers(self, order_numbers: Tuple[Order.Number]) -> Tuple[Order]:
        if any([not isinstance(order_number, Order.Number) for order_number in order_numbers]):
            raise ArgumentTypeException(self.get_all_by_numbers, 'order_numbers', order_numbers)

        result = [self.load(order_number) for order_number in order_numbers]
        result = [order for order in result if order is not None]
        return tuple(result)

    def get_all_for_customer(self, customer_id: Id) -> Tuple[Order]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.get_all_for_customer, 'customer_id', customer_id)

        items = self.__dynamo_db.find_by_attribute('customer_id', customer_id.value)
        result = [self.__restore(item) for item in items]
        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class OrderStorageImplementation(OrderStorageInterface):
    def __init__(self):
        self.__storage = _OrderStorageDynamoDb()

    def save(self, order: Order) -> None:
        return self.__storage.save(order)

    def load(self, order_number: Order.Number) -> Optional[Order]:
        return self.__storage.load(order_number)

    def get_all_by_numbers(self, order_numbers: Tuple[Order.Number]) -> Tuple[Order]:
        return self.__storage.get_all_by_numbers(order_numbers)

    def get_all_for_customer(self, customer_id: Id) -> Tuple[Order]:
        return self.__storage.get_all_for_customer(customer_id)


# ----------------------------------------------------------------------------------------------------------------------

