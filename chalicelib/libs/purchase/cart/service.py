from chalicelib.extensions import *
from chalicelib.libs.purchase.core import \
    ProductStorageInterface, Cart, CartStorageInterface, \
    Id, Qty, SimpleSku, Percentage
from chalicelib.libs.purchase.settings import PurchaseSettings
from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
from chalicelib.libs.purchase.cart.storage import CartStorageImplementation


# instead of di-container, factories, etc.
class CartAppService(object):
    def __init__(self):
        self.__service = _CartAppService(
            ProductStorageImplementation(),
            CartStorageImplementation(),
            Percentage(PurchaseSettings().vat)
        )

    def add_cart_product(self, cart_id: str, simple_sku: str, qty: int) -> None:
        cart_id = Id(cart_id)
        simple_sku = SimpleSku(simple_sku)
        qty = Qty(qty)
        self.__service.add_cart_product(cart_id, simple_sku, qty)

    def set_cart_product_qty(self, cart_id: str, simple_sku: str, qty: int) -> None:
        cart_id = Id(cart_id)
        simple_sku = SimpleSku(simple_sku)
        qty = Qty(qty)
        self.__service.set_cart_product_qty(cart_id, simple_sku, qty)

    def remove_cart_product(self, cart_id: str, simple_sku: str) -> None:
        cart_id = Id(cart_id)
        simple_sku = SimpleSku(simple_sku)
        self.__service.remove_cart_product(cart_id, simple_sku)

    def clear_cart(self, cart_id: str) -> None:
        cart_id = Id(cart_id)
        self.__service.clear_cart(cart_id)

    def transfer_cart(self, source_cart_id: str, destination_cart_id: str) -> None:
        source_cart_id = Id(source_cart_id)
        destination_cart_id = Id(destination_cart_id)
        self.__service.transfer_cart(source_cart_id, destination_cart_id)


# ----------------------------------------------------------------------------------------------------------------------


class _CartAppService(object):
    def __init__(
        self,
        products_storage: ProductStorageInterface,
        cart_storage: CartStorageInterface,
        vat_percent: Percentage
    ):
        if not isinstance(products_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'products_storage', products_storage)

        if not isinstance(cart_storage, CartStorageInterface):
            raise ArgumentTypeException(self.__init__, 'cart_storage', cart_storage)

        if not isinstance(vat_percent, Percentage):
            raise ArgumentTypeException(self.__init__, 'vat_percent', vat_percent)

        self.__products_storage = products_storage
        self.__cart_storage = cart_storage
        self.__vat_percent = vat_percent

    def __load_or_create_cart(self, cart_id: Id) -> Cart:
        cart = self.__cart_storage.get_by_id(cart_id)
        cart = cart if cart else Cart(cart_id, self.__vat_percent)
        return cart

    def add_cart_product(self, cart_id: Id, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.add_cart_product, 'cart_id', cart_id)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.add_cart_product, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.add_cart_product, 'qty', qty)

        product = self.__products_storage.load(simple_sku)
        if not product:
            raise ApplicationLogicException('Product "{}" does not exist!'.format(simple_sku.value))

        cart = self.__load_or_create_cart(cart_id)
        cart.add_item(product, qty)
        self.__cart_storage.save(cart)

    def set_cart_product_qty(self, cart_id: Id, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.set_cart_product_qty, 'cart_id', cart_id)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.set_cart_product_qty, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.set_cart_product_qty, 'qty', qty)

        cart = self.__load_or_create_cart(cart_id)
        cart.set_item_qty(simple_sku, qty)
        self.__cart_storage.save(cart)

    def remove_cart_product(self, cart_id: Id, simple_sku: SimpleSku) -> None:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.remove_cart_product, 'cart_id', cart_id)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.remove_cart_product, 'simple_sku', simple_sku)

        cart = self.__load_or_create_cart(cart_id)
        cart.remove_item(simple_sku)
        self.__cart_storage.save(cart)

    def clear_cart(self, cart_id: Id) -> None:
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.clear_cart, 'cart_id', cart_id)

        cart = self.__load_or_create_cart(cart_id)
        cart.clear()
        self.__cart_storage.save(cart)

    def transfer_cart(self, source_cart_id: Id, destination_cart_id: Id) -> None:
        if not isinstance(source_cart_id, Id):
            raise ArgumentTypeException(self.transfer_cart, 'source_cart_id', source_cart_id)

        if not isinstance(destination_cart_id, Id):
            raise ArgumentTypeException(self.transfer_cart, 'destination_cart_id', destination_cart_id)

        source_cart = self.__load_or_create_cart(source_cart_id)
        destination_cart = self.__load_or_create_cart(destination_cart_id)

        for source_cart_item in source_cart.items:
            destination_cart.add_item(source_cart_item.product, source_cart_item.qty)

        source_cart.clear()

        self.__cart_storage.save(destination_cart)
        self.__cart_storage.save(source_cart)


# ----------------------------------------------------------------------------------------------------------------------

