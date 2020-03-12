from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.purchase.core import \
    Cart, CartStorageInterface, \
    ProductStorageInterface, \
    Id, SimpleSku, Qty, Percentage
from chalicelib.libs.purchase.settings import PurchaseSettings
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation


# instead of di-container, factories, etc.
class CartStorageImplementation(CartStorageInterface):
    def __init__(self):
        self.__storage = _CartDynamoDbStorage(
            ProductStorageImplementation()
        )

    def save(self, cart: Cart) -> None:
        return self.__storage.save(cart)

    def get_by_id(self, cart_id: Id) -> Optional[Cart]:
        return self.__storage.get_by_id(cart_id)


# ----------------------------------------------------------------------------------------------------------------------


class _CartDynamoDbStorage(DynamoModel, CartStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PURCHASE_CART'

    __ENTITY_PROPERTY_ID = '__id'
    __ENTITY_PROPERTY_ITEMS = '__items'
    __ENTITY_PROPERTY_ITEMS_PRODUCT = '__product'
    __ENTITY_PROPERTY_ITEMS_QTY = '__qty'
    __ENTITY_PROPERTY_VAT_PERCENT = '__vat_percent'

    def __init__(self, product_storage: ProductStorageInterface):
        if not isinstance(product_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'product_storage', product_storage)

        super(self.__class__, self).__init__(self.TABLE_NAME)
        self.__product_storage = product_storage
        self.__vat_percent = Percentage(PurchaseSettings().vat)
        self.__reflector = Reflector()

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

    def get_by_id(self, cart_id: Id) -> Optional[Cart]:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.get_by_id, 'cart_id', cart_id)

        data = self.get_item(cart_id.value).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data: dict) -> Cart:
        cart_items = []
        for item_data in data.get('cart_items', tuple()):
            simple_sku = SimpleSku(str(item_data.get('simple_sku')))
            qty = Qty(int(item_data.get('qty')))
            product = self.__product_storage.load(simple_sku)
            cart_items.append(self.__reflector.construct(Cart.Item, {
                self.__class__.__ENTITY_PROPERTY_ITEMS_PRODUCT: product,
                self.__class__.__ENTITY_PROPERTY_ITEMS_QTY: qty,
            }))

        cart: Cart = self.__reflector.construct(Cart, {
            self.__class__.__ENTITY_PROPERTY_ID: Id(data.get('sk')),
            self.__class__.__ENTITY_PROPERTY_ITEMS: cart_items,
            self.__class__.__ENTITY_PROPERTY_VAT_PERCENT: self.__vat_percent
        })

        return cart


# ----------------------------------------------------------------------------------------------------------------------

