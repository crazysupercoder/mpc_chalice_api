from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.purchase.core import \
    Id, SimpleSku, Qty, DeliveryAddress, Cost, Percentage,\
    CustomerStorageInterface, ProductStorageInterface, \
    Checkout, CheckoutStorageInterface


# @todo : refactoring : dynamo db composition

class _CheckoutDynamoDbStorage(DynamoModel, CheckoutStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PURCHASE_CHECKOUT'

    def __init__(
        self,
        customer_storage: CustomerStorageInterface,
        product_storage: ProductStorageInterface,
    ):
        if not isinstance(customer_storage, CustomerStorageInterface):
            raise ArgumentTypeException(self.__init__, 'customer_storage', customer_storage)

        if not isinstance(product_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'product_storage', product_storage)

        super(self.__class__, self).__init__(self.TABLE_NAME)
        self.__customer_storage = customer_storage
        self.__product_storage = product_storage

    # ------------------------------------------------------------------------------------------------------------------

    def save(self, checkout: Checkout) -> None:
        if not isinstance(checkout, Checkout):
            raise ArgumentTypeException(self.save, 'checkout', checkout)

        data = {
            'pk': self.PARTITION_KEY,
            'sk': checkout.customer_id.value,
            'credits_available_amount': checkout.credits_amount_available.value,
            'is_credits_in_use': 1 if checkout.is_credits_in_use else 0,
            'checkout_items': [{
                'simple_sku': checkout_item.simple_sku.value,
                'qty': checkout_item.qty.value,
            } for checkout_item in checkout.checkout_items],
            'delivery_address': {
                'recipient_name': checkout.delivery_address.recipient_name,
                'phone_number': checkout.delivery_address.phone_number,
                'street_address': checkout.delivery_address.street_address,
                'suburb': checkout.delivery_address.suburb,
                'city': checkout.delivery_address.city,
                'province': checkout.delivery_address.province,
                'complex_building': checkout.delivery_address.complex_building,
                'postal_code': checkout.delivery_address.postal_code,
                'business_name': checkout.delivery_address.business_name,
                'special_instructions': checkout.delivery_address.special_instructions,
            } if checkout.delivery_address else None,
            'delivery_cost': checkout.delivery_cost.value,
            'vat_percent': checkout.vat_percent.value,
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        import json
        from decimal import Decimal
        data = json.loads(json.dumps(data), parse_float=Decimal)

        # insert or update
        self.table.put_item(Item=data)

    # ------------------------------------------------------------------------------------------------------------------

    def load(self, customer_id: Id) -> Optional[Checkout]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.load, 'customer_id', customer_id)

        data = self.get_item(customer_id.value).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data) -> Checkout:
        customer_id = Id(data.get('sk'))
        customer = self.__customer_storage.get_by_id(customer_id)

        checkout_items = []
        for item in data.get('checkout_items', tuple()):
            simple_sku = SimpleSku(str(item.get('simple_sku')))
            qty = Qty(int(item.get('qty')))

            product = self.__product_storage.load(simple_sku)
            checkout_item = Checkout.Item(product, qty)
            checkout_items.append(checkout_item)

        delivery_address = DeliveryAddress(
            data.get('delivery_address').get('recipient_name'),
            data.get('delivery_address').get('phone_number'),
            data.get('delivery_address').get('street_address'),
            data.get('delivery_address').get('suburb'),
            data.get('delivery_address').get('city'),
            data.get('delivery_address').get('province'),
            data.get('delivery_address').get('complex_building'),
            data.get('delivery_address').get('postal_code'),
            data.get('delivery_address').get('business_name'),
            data.get('delivery_address').get('special_instructions')
        ) if data.get('delivery_address', None) else None

        delivery_cost = Cost(float(data.get('delivery_cost')))
        vat = Percentage(float(data.get('vat_percent')))

        # can not exist for old data
        available_credits_amount = Cost(float(data.get('credits_available_amount', '0') or '0'))
        is_credits_in_use = bool(int(data.get('is_credits_in_use', '0') or '0'))

        # @todo : reflection
        checkout = object.__new__(Checkout)
        checkout._Checkout__customer = customer
        checkout._Checkout__checkout_items = checkout_items
        checkout._Checkout__delivery_address = delivery_address
        checkout._Checkout__delivery_cost = delivery_cost
        checkout._Checkout__vat_percent = vat
        checkout._Checkout__available_credits_amount = available_credits_amount
        checkout._Checkout__is_credits_in_use = is_credits_in_use

        return checkout

    # ------------------------------------------------------------------------------------------------------------------

    def remove(self, customer_id: Id) -> None:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.remove, 'customer_id', customer_id)

        key = {'pk': self.PARTITION_KEY, 'sk': customer_id.value}
        self.table.delete_item(Key=key)


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CheckoutStorageImplementation(CheckoutStorageInterface):
    def __init__(self):
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
        self.__storage = _CheckoutDynamoDbStorage(
            CustomerStorageImplementation(),
            ProductStorageImplementation()
        )

    def save(self, checkout: Checkout) -> None:
        return self.__storage.save(checkout)

    def load(self, customer_id: Id) -> Optional[Checkout]:
        return self.__storage.load(customer_id)

    def remove(self, customer_id: Id) -> None:
        return self.__storage.remove(customer_id)


# ----------------------------------------------------------------------------------------------------------------------

