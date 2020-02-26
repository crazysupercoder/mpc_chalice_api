from .base import *
from uuid import uuid4
from datetime import datetime
from chalicelib.settings import settings
from chalicelib.extensions import *
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.user import User


# @todo : move somewhere, update or remove, if it will not be needed.

class OrderItem:
    def __init__(
        self,
        order_number: str,
        customer_email: str,
        ordered_at: datetime,
        product_sku: str,
        product_size_sku: str,
        product_name: str,
        product_brand_name: str,
        product_gender_name: str,
        product_type_name: str,
        product_color_name: str,
        product_size_name: str,
        qty_ordered: int
    ):
        if not isinstance(ordered_at, datetime):
            raise ArgumentTypeException(self.__init__, 'ordered_at', ordered_at)

        if not isinstance(qty_ordered, int):
            raise ArgumentTypeException(self.__init__, 'qty_ordered', qty_ordered)
        elif qty_ordered < 0:
            raise ArgumentValueException('{} parameter {} must be >= 0, {} is given'.format(
                self.__init__.__qualname__,
                'qty_ordered',
                qty_ordered
            ))

        required_str_attributes = {
            'order_number': order_number,
            'customer_email': customer_email,
            'product_sku': product_sku,
            'product_size_sku': product_size_sku,
            'product_name': product_name,
            'product_brand_name': product_brand_name,
            'product_gender_name': product_gender_name,
            'product_type_name': product_type_name,
            'product_color_name': product_color_name,
            'product_size_name': product_size_name,
        }
        for attribute_name in tuple(required_str_attributes.keys()):
            attribute_value = required_str_attributes[attribute_name]
            if not isinstance(attribute_value, str):
                raise ArgumentTypeException(self.__init__, attribute_name, attribute_value)
            elif not str(attribute_value).strip():
                raise ArgumentCannotBeEmptyException(self.__init__, attribute_name)

        self.__order_number = str(order_number).strip()
        self.__ordered_at = ordered_at
        self.__qty_ordered = qty_ordered
        self.__customer_email = str(customer_email).strip()
        self.__product_sku = str(product_sku).strip()
        self.__product_size_sku = str(product_size_sku).strip()
        self.__product_name = str(product_name).strip()
        self.__product_brand_name = str(product_brand_name).strip()
        self.__product_gender_name = str(product_gender_name).strip()
        self.__product_type_name = str(product_type_name).strip()
        self.__product_color_name = str(product_color_name).strip()
        self.__product_size_name = str(product_size_name).strip()

    @property
    def order_number(self) -> str:
        return self.__order_number

    @property
    def customer_email(self) -> str:
        return self.__customer_email

    @property
    def ordered_at(self) -> datetime:
        return self.__ordered_at

    @property
    def product_sku(self) -> str:
        return self.__product_sku

    @property
    def product_size_sku(self) -> str:
        return self.__product_size_sku

    @property
    def product_name(self) -> str:
        return self.__product_name

    @property
    def product_brand_name(self) -> str:
        return self.__product_brand_name

    @property
    def product_gender_name(self) -> str:
        return self.__product_gender_name

    @property
    def product_type_name(self) -> str:
        return self.__product_type_name

    @property
    def product_color_name(self) -> str:
        return self.__product_color_name

    @property
    def product_size_name(self) -> str:
        return self.__product_size_name

    @property
    def qty_ordered(self) -> int:
        return self.__qty_ordered


class OrderHandler(SqsHandlerInterface):
    def __init__(self) -> None:
        """
            curl -X DELETE http://localhost:9200/personalization_orders
            curl -X PUT http://localhost:9200/personalization_orders -H "Content-Type: application/json" -d'{
                "mappings": {
                    "personalization_orders": {
                        "properties": {
                            "order_number": {"type": "keyword"},
                            "email": {"type": "keyword"},
                            "ordered_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                            "rs_sku": {"type": "keyword"},
                            "rs_simple_sku": {"type": "keyword"},
                            "product_name": {"type": "keyword"},
                            "manufacturer": {"type": "keyword"},
                            "gender": {"type": "keyword"},
                            "product_size_attribute": {"type": "keyword"},
                            "rs_colour": {"type": "keyword"},
                            "size": {"type": "keyword"}
                        }
                    }
                 }
            }'
        """
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS,
            settings.AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS
        )

    def handle(self, sqs_message: SqsMessage) -> None:
        order_number = str(sqs_message.message_data.get('order_number') or '').strip() or None
        order_items = sqs_message.message_data.get('order_items', []) or []
        if not order_number or not order_items:
            raise ValueError('{} message data is incorrect: {}'.format(
                self.handle.__qualname__,
                sqs_message.message_data
            ))

        try:
            self.__elastic.delete_by_query({
                "query": {
                    "term": {
                        "order_number": order_number
                    }
                }
            })

            emails = list()

            for order_item_data in order_items:
                emails.append(order_item_data.get('customer_email'))
                order_item = OrderItem(
                    order_number,
                    order_item_data.get('customer_email'),
                    datetime.strptime(order_item_data.get('order_created_at'), '%Y-%m-%d %H:%M:%S'),
                    order_item_data.get('product_config_sku'),
                    order_item_data.get('product_simple_sku'),
                    order_item_data.get('product_name'),
                    order_item_data.get('product_brand_name'),
                    order_item_data.get('product_gender_name'),
                    order_item_data.get('product_type_name'),
                    order_item_data.get('product_color_name'),
                    order_item_data.get('product_size_name'),
                    int(order_item_data.get('qty_ordered')),
                )

                document_id = str(uuid4())
                self.__elastic.create(document_id, {
                    'order_number': order_number,
                    'email': order_item.customer_email,
                    'ordered_at': order_item.ordered_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'rs_sku': order_item.product_sku,
                    'rs_simple_sku': order_item.product_size_sku,
                    'product_name': order_item.product_name,
                    'manufacturer': order_item.product_brand_name,
                    'gender': order_item.product_gender_name,
                    'product_size_attribute': order_item.product_type_name,
                    'rs_colour': order_item.product_color_name,
                    'size': order_item.product_size_name,
                })
            
            emails = list(set(emails))
            User.send_calculate_product_score_for_customers(emails=emails)
        except BaseException as e:
            raise RuntimeError('{} got an error, when tried to handle order {}. Error: {}'.format(
                self.handle.__qualname__,
                sqs_message.message_data,
                e
            ))

