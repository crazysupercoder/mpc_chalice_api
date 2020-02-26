from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.purchase.core.cart import Cart, CartItem, CartStorageInterface
from chalicelib.libs.purchase.core.product import ProductStorageInterface
from chalicelib.libs.purchase.core.values import Id, SimpleSku, Qty, Percentage
from chalicelib.libs.purchase.settings import PurchaseSettings


class _CartDynamoDbStorage(DynamoModel, CartStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PURCHASE_CART'

    def __init__(self, product_storage: ProductStorageInterface):
        if not isinstance(product_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'product_storage', product_storage)

        super(self.__class__, self).__init__(self.TABLE_NAME)
        self.__product_storage = product_storage
        self.__vat_percent = Percentage(PurchaseSettings().vat)

    # ------------------------------------------------------------------------------------------------------------------

    def save(self, cart: Cart) -> None:
        if not isinstance(cart, Cart):
            raise ArgumentTypeException(self.save, 'cart', cart)

        data = {
            'pk': self.PARTITION_KEY,
            'sk': cart.cart_id.value,
            'cart_items': [{
                'simple_sku': item.simple_sku.value,
                'qty': item.qty.value,
            } for item in cart.items]
        }

        # insert or update
        self.table.put_item(Item=data)

    # ------------------------------------------------------------------------------------------------------------------

    def load(self, cart_id: Id) -> Optional[Cart]:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.load, 'cart_id', cart_id)

        data = self.get_item(cart_id.value).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data) -> Cart:
        cart_id = Id(data.get('sk'))

        cart_items = []
        for item_data in data.get('cart_items', tuple()):
            simple_sku = SimpleSku(str(item_data.get('simple_sku')))
            qty = Qty(int(item_data.get('qty')))
            product = self.__product_storage.load(simple_sku)
            cart_items.append(CartItem(product, qty))

        cart = object.__new__(Cart)
        cart._Cart__id = cart_id
        cart._Cart__items = cart_items
        cart._Cart__vat_percent = self.__vat_percent

        return cart


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CartStorageImplementation(CartStorageInterface):
    def __init__(self):
        from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
        self.__storage = _CartDynamoDbStorage(ProductStorageImplementation())

    def save(self, cart: Cart) -> None:
        return self.__storage.save(cart)

    def load(self, cart_id: Id) -> Optional[Cart]:
        return self.__storage.load(cart_id)


# ----------------------------------------------------------------------------------------------------------------------

