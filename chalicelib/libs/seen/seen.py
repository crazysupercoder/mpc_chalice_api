from typing import Tuple, List, Optional
from chalicelib.extensions import *


# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


class ProductNotInSeenException(ApplicationLogicException):
    def __init__(self, sku):
        super().__init__('Product "' + str(sku) + '" is not added to the Seen!')


# ----------------------------------------------------------------------------------------------------------------------


class Seen(object):
    def __init__(self, seen_id):
        if seen_id is None:
            raise ArgumentTypeException(self.__init__, 'seen_id', seen_id)

        self.__id = seen_id
        self.__items = []

    @property
    def seen_id(self):
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
            raise ProductNotInSeenException(sku)

    def clear(self) -> None:
        self.__items = []

    def is_added(self, sku) -> bool:
        for old_item in self.__items:
            if old_item == sku:
                return True
        return False

# ----------------------------------------------------------------------------------------------------------------------


class SeenStorageInterface(object):
    def save(self, seen: Seen) -> None:
        raise NotImplementedError()

    def load(self, seen_id) -> Optional[Seen]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

