from typing import Tuple, List, Optional
from chalicelib.extensions import *
from .values import Id, SimpleSku, Qty, Cost, Percentage
from .product import ProductInterface


# ----------------------------------------------------------------------------------------------------------------------


class _CartItem(object):
    def __init__(self, product: ProductInterface, qty: Qty):
        if not isinstance(product, ProductInterface):
            raise ArgumentTypeException(self.__init__, 'product', product)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.__init__, 'qty', qty)
        elif qty.value < 1:
            raise ArgumentValueException('{} expects qty > 0'.format(self.__init__.__qualname__))

        self.__product = product
        self.__qty = qty

    @property
    def product(self) -> ProductInterface:
        return self.__product

    @property
    def simple_sku(self) -> SimpleSku:
        return self.__product.simple_sku

    @property
    def product_original_price(self) -> Cost:
        return self.__product.original_price

    @property
    def product_current_price(self) -> Cost:
        return self.__product.current_price

    @property
    def qty(self) -> Qty:
        return self.__qty

    @property
    def original_cost(self) -> Cost:
        return Cost(self.__qty.value * self.__product.original_price.value)

    @property
    def current_cost(self) -> Cost:
        return Cost(self.__qty.value * self.__product.current_price.value)

    @property
    def is_added_over_limit(self) -> bool:
        return self.__qty.value > self.__product.qty_available.value


# ----------------------------------------------------------------------------------------------------------------------


class _ProductNotInCartException(ApplicationLogicException):
    def __init__(self, simple_sku: SimpleSku):
        super().__init__('Product "' + str(simple_sku) + '" is not added to the Cart!')


# ----------------------------------------------------------------------------------------------------------------------


class Cart(object):
    Item = _CartItem

    def __init__(self, cart_id: Id, vat_percent: Percentage):
        if not isinstance(cart_id, Id):
            raise ArgumentTypeException(self.__init__, 'cart_id', cart_id)

        if not isinstance(vat_percent, Percentage):
            raise ArgumentTypeException(self.__init__, 'vat_percent', vat_percent)

        self.__id = cart_id
        self.__items: List[Cart.Item] = []
        self.__vat_percent = vat_percent

    @property
    def cart_id(self) -> Id:
        return self.__id

    @property
    def items(self) -> Tuple['Cart.Item']:
        return tuple(self.__items)

    @property
    def is_empty(self) -> bool:
        return len(self.__items) == 0

    @property
    def has_products_added_over_limit(self) -> bool:
        return bool(sum([item.is_added_over_limit for item in self.__items]))

    @property
    def original_subtotal(self) -> Cost:
        return Cost(sum([item.original_cost.value for item in self.__items]))

    @property
    def current_subtotal(self) -> Cost:
        return Cost(sum([item.current_cost.value for item in self.__items]))

    @property
    def current_subtotal_vat_amount(self) -> Cost:
        per_percent = self.current_subtotal.value / (100 + self.__vat_percent.value)
        return Cost(per_percent * self.__vat_percent.value)

    def add_item(self, product: ProductInterface, qty: Qty) -> None:
        new_item = Cart.Item(product, qty)

        for old_item in self.__items:
            if old_item.simple_sku == new_item.simple_sku:
                self.set_item_qty(old_item.simple_sku, old_item.qty + new_item.qty)
                return
        else:
            self.__items.append(new_item)

    def set_item_qty(self, simple_sku: SimpleSku, qty: Qty) -> None:
        for index, item in enumerate(self.__items):
            if item.simple_sku == simple_sku:
                self.__items[index] = Cart.Item(item.product, qty)
                return
        else:
            raise _ProductNotInCartException(simple_sku)

    def remove_item(self, simple_sku: SimpleSku) -> None:
        for index, item in enumerate(self.__items):
            if item.simple_sku == simple_sku:
                del self.__items[index]
                return
        else:
            raise _ProductNotInCartException(simple_sku)

    def clear(self) -> None:
        self.__items = []


# ----------------------------------------------------------------------------------------------------------------------


class CartStorageInterface(object):
    def save(self, cart: Cart) -> None:
        raise NotImplementedError()

    def get_by_id(self, cart_id: Id) -> Optional[Cart]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

