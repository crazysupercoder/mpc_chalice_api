from chalicelib.extensions import *
from chalicelib.libs.purchase.core.product import ProductStorageInterface
from chalicelib.libs.purchase.core.cart import Cart, CartStorageInterface
from chalicelib.libs.purchase.core.values import Id, Qty, SimpleSku, Percentage
from chalicelib.libs.purchase.settings import PurchaseSettings


class _CartAppService(object):
    def __init__(
        self,
        products_storage: ProductStorageInterface,
        cart_storage: CartStorageInterface
    ):
        if not isinstance(products_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'products_storage', products_storage)

        if not isinstance(cart_storage, CartStorageInterface):
            raise ArgumentTypeException(self.__init__, 'cart_storage', cart_storage)

        self.__products_storage = products_storage
        self.__cart_storage = cart_storage
        self.__vat_percent = Percentage(PurchaseSettings().vat)

    # ------------------------------------------------------------------------------------------------------------------

    def __load_or_create_cart(self, session_id: str) -> Cart:
        cart_id = Id(session_id)
        cart = self.__cart_storage.load(cart_id)
        cart = cart if cart else Cart(cart_id, self.__vat_percent)
        return cart

    # ------------------------------------------------------------------------------------------------------------------

    def add_cart_product(self, session_id: str, simple_sku: str, qty: int) -> None:
        _simple_sku = SimpleSku(simple_sku)
        _qty = Qty(qty)

        product = self.__products_storage.load(_simple_sku)
        if not product:
            raise ApplicationLogicException('Product "{0}" does not exist!'.format(_simple_sku))

        cart = self.__load_or_create_cart(session_id)
        cart.add_item(product, _qty)
        self.__cart_storage.save(cart)

    # ------------------------------------------------------------------------------------------------------------------

    def set_cart_product_qty(self, session_id: str, simple_sku: str, qty: int) -> None:
        _simple_sku = SimpleSku(simple_sku)
        _qty = Qty(qty)

        cart = self.__load_or_create_cart(session_id)
        cart.set_item_qty(_simple_sku, _qty)
        self.__cart_storage.save(cart)

    # ------------------------------------------------------------------------------------------------------------------

    def remove_cart_product(self, session_id: str, simple_sku: str) -> None:
        _simple_sku = SimpleSku(simple_sku)

        cart = self.__load_or_create_cart(session_id)
        cart.remove_item(_simple_sku)
        self.__cart_storage.save(cart)

    # ------------------------------------------------------------------------------------------------------------------

    def clear_cart(self, session_id) -> None:
        cart = self.__load_or_create_cart(session_id)
        cart.clear()
        self.__cart_storage.save(cart)


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CartAppService(_CartAppService):
    def __init__(self):
        from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
        from chalicelib.libs.purchase.cart.storage import CartStorageImplementation
        super().__init__(
            ProductStorageImplementation(),
            CartStorageImplementation()
        )


# ----------------------------------------------------------------------------------------------------------------------

