from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.wish.wish import Wish, WishStorageInterface
from chalicelib.libs.models.mpc.Product import Product

class _WishDynamoDbStorage(DynamoModel, WishStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'WISH'

    def __init__(self, product: Product):
        if not isinstance(product, Product):
            raise ArgumentTypeException(self.__init__, 'product', product)

        super(self.__class__, self).__init__(self.TABLE_NAME)
        self.__product = product

    # ------------------------------------------------------------------------------------------------------------------

    def save(self, wish: Wish) -> None:
        if not isinstance(wish, Wish):
            raise ArgumentTypeException(self.save, 'wish', wish)

        data = {
            'pk': self.PARTITION_KEY,
            'sk': wish.wish_id,
            'wish_items': wish.items
        }

        # insert or update
        self.table.put_item(Item=data)

    # ------------------------------------------------------------------------------------------------------------------

    def load(self, wish_id) -> Optional[Wish]:
        if wish_id is None:
            raise ArgumentTypeException(self.load, 'wish_id', wish_id)

        data = self.get_item(wish_id).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data) -> Wish:
        wish_id = data.get('sk')

        wish = object.__new__(Wish)
        wish._Wish__id = wish_id
        wish._Wish__items = data.get('wish_items', [])

        return wish


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class WishStorageImplementation(WishStorageInterface):
    def __init__(self):
        self.__storage = _WishDynamoDbStorage(Product())

    def save(self, wish: Wish) -> None:
        return self.__storage.save(wish)

    def load(self, wish_id) -> Optional[Wish]:
        return self.__storage.load(wish_id)


# ----------------------------------------------------------------------------------------------------------------------

