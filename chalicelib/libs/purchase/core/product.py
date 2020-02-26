from typing import Optional, Tuple
from .values import EventCode, SimpleSku, Qty, Cost, Name


# ----------------------------------------------------------------------------------------------------------------------


class ProductInterface(object):
    @property
    def event_code(self) -> EventCode:
        raise NotImplementedError()

    @property
    def simple_sku(self) -> SimpleSku:
        raise NotImplementedError()

    @property
    def original_price(self) -> Cost:
        raise NotImplementedError()

    @property
    def current_price(self) -> Cost:
        raise NotImplementedError()

    @property
    def qty_available(self) -> Qty:
        raise NotImplementedError()

    @property
    def name(self) -> Name:
        raise NotImplementedError()

    @property
    def size_name(self) -> Name:
        raise NotImplementedError()

    @property
    def image_urls(self) -> Tuple[str]:
        raise NotImplementedError()

    def sell_qty(self, qty: Qty) -> None:
        raise NotImplementedError()

    def restore_qty(self, qty: Qty) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class ProductStorageInterface(object):
    def load(self, simple_sku: SimpleSku) -> Optional[ProductInterface]:
        raise NotImplementedError()

    def update(self, product: ProductInterface) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

