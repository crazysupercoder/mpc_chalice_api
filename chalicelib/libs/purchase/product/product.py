from typing import Tuple
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import ProductInterface, EventCode, SimpleSku, Qty, Cost, Name


class ProductInterfaceImplementation(ProductInterface):
    def __init__(self, product_data: dict, simple_sku: str):
        if not isinstance(product_data, dict):
            raise ArgumentTypeException(self.__init__, 'product_data', product_data)

        if not isinstance(simple_sku, str):
            raise ArgumentTypeException(self.__init__, 'simple_sku', simple_sku)

        for size in product_data.get('sizes', ()):
            if size.get('simple_sku') == simple_sku:
                simple_data = size
                break
        else:
            raise ValueError('{0} cannot be created: simple sku {1} is not related to product {2}'.format(
                self.__class__.__name__,
                simple_sku,
                product_data
            ))

        # @todo : refactoring
        original_price = float(product_data.get('price'))
        discount = float(product_data.get('discount'))
        current_price = original_price - original_price * discount / 100

        self.__event_code = EventCode(str(product_data.get('event_code')))
        self.__simple_sku = SimpleSku(str(simple_data.get('simple_sku')))
        self.__qty = Qty(int(simple_data.get('qty')))
        self.__original_price = Cost(original_price)
        self.__current_price = Cost(current_price)
        self.__name = Name(product_data.get('title'))
        self.__size_name = Name(simple_data.get('size'))
        self.__image_urls = tuple([
            product_data['image']['src']
        ])

    @property
    def event_code(self) -> EventCode:
        return self.__event_code

    @property
    def simple_sku(self) -> SimpleSku:
        return self.__simple_sku

    @property
    def original_price(self) -> Cost:
        return self.__original_price

    @property
    def current_price(self) -> Cost:
        return self.__current_price

    @property
    def qty_available(self) -> Qty:
        return self.__qty

    @property
    def name(self) -> Name:
        return clone(self.__name)

    @property
    def size_name(self) -> Name:
        return clone(self.__size_name)

    @property
    def image_urls(self) -> Tuple[str]:
        return clone(self.__image_urls)

    def sell_qty(self, qty: Qty) -> None:
        self.__qty = self.__qty - qty

    def restore_qty(self, qty: Qty) -> None:
        self.__qty = self.__qty + qty

