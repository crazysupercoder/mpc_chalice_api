from typing import Tuple, List, Optional
from chalicelib.extensions import *


# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


class ProductNotInWishException(ApplicationLogicException):
    def __init__(self, sku):
        super().__init__('Product "' + str(sku) + '" is not added to the Wish!')


# ----------------------------------------------------------------------------------------------------------------------


class Wish(object):
    def __init__(self, wish_id):
        if wish_id is None:
            raise ArgumentTypeException(self.__init__, 'wish_id', wish_id)

        self.__id = wish_id
        self.__items = []

    @property
    def wish_id(self):
        return self.__id

    @property
    def items(self):
        return self.__items

    @property
    def is_empty(self) -> bool:
        return len(self.__items) == 0

    def add_item(self, sku) -> None:

        for old_item in self.__items:
            if old_item == sku:
                return
        else:
            self.__items.append(sku)

    def remove_item(self, sku) -> None:
        for index, item in enumerate(self.__items):
            if item == sku:
                del self.__items[index]
                return
        else:
            raise ProductNotInWishException(sku)

    def clear(self) -> None:
        self.__items = []


# ----------------------------------------------------------------------------------------------------------------------


class WishStorageInterface(object):
    def save(self, wish: Wish) -> None:
        raise NotImplementedError()

    def load(self, wish_id) -> Optional[Wish]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

