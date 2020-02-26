from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.seen.seen import Seen, SeenStorageInterface
from chalicelib.libs.models.mpc.Product import Product

class _SeenDynamoDbStorage(DynamoModel, SeenStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'SEEN'

    def __init__(self, product: Product):
        if not isinstance(product, Product):
            raise ArgumentTypeException(self.__init__, 'product', product)

        super(self.__class__, self).__init__(self.TABLE_NAME)
        self.__product = product

    # ------------------------------------------------------------------------------------------------------------------

    def save(self, seen: Seen) -> None:
        if not isinstance(seen, Seen):
            raise ArgumentTypeException(self.save, 'seen', seen)

        data = {
            'pk': self.PARTITION_KEY,
            'sk': seen.seen_id,
            'seen_items': seen.items
        }

        # insert or update
        self.table.put_item(Item=data)

    # ------------------------------------------------------------------------------------------------------------------

    def load(self, seen_id) -> Optional[Seen]:
        if seen_id is None:
            raise ArgumentTypeException(self.load, 'seen_id', seen_id)

        data = self.get_item(seen_id).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data) -> Seen:
        seen_id = data.get('sk')

        seen = object.__new__(Seen)
        seen._Seen__id = seen_id
        seen._Seen__items = data.get('seen_items', [])

        return seen


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class SeenStorageImplementation(SeenStorageInterface):
    def __init__(self):
        self.__storage = _SeenDynamoDbStorage(Product())

    def save(self, seen: Seen) -> None:
        return self.__storage.save(seen)

    def load(self, seen_id) -> Optional[Seen]:
        return self.__storage.load(seen_id)


# ----------------------------------------------------------------------------------------------------------------------

