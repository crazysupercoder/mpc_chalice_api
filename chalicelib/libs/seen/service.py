from chalicelib.extensions import *
from chalicelib.libs.seen.seen import Seen, SeenStorageInterface
from chalicelib.libs.models.mpc.Product import Product

class _SeenAppService(object):
    def __init__(
        self,
        seen_storage: SeenStorageInterface
    ):

        if not isinstance(seen_storage, SeenStorageInterface):
            raise ArgumentTypeException(self.__init__, 'seen_storage', seen_storage)

        self.__seen_storage = seen_storage
        self.__products = Product()

    # ------------------------------------------------------------------------------------------------------------------

    def __load_or_create_seen(self, user_id: str) -> Seen:
        seen = self.__seen_storage.load(user_id)
        seen = seen if seen else Seen(user_id)
        return seen

    # ------------------------------------------------------------------------------------------------------------------

    def add_seen_product(self, user_id: str, sku: str) -> None:

        # product = self.__products.get_raw_data(sku, True)
        # if not product:
        #     raise ApplicationLogicException('Product "{0}" does not exist!'.format(sku))

        seen = self.__load_or_create_seen(user_id)
        seen.add_item(sku)
        self.__seen_storage.save(seen)

    def add_seen_products(self, user_id: str, skus: list) -> None:

        seen = self.__load_or_create_seen(user_id)
        for sku in skus:
            seen.add_item(sku)
        self.__seen_storage.save(seen)

    # ------------------------------------------------------------------------------------------------------------------

    def remove_seen_product(self, user_id: str, sku: str) -> None:

        seen = self.__load_or_create_seen(user_id)
        seen.remove_item(sku)
        self.__seen_storage.save(seen)

    # ------------------------------------------------------------------------------------------------------------------

    def clear_seen(self, user_id) -> None:
        seen = self.__load_or_create_seen(user_id)
        seen.clear()
        self.__seen_storage.save(seen)

    # ------------------------------------------------------------------------------------------------------------------

    def product_is_in_seen(self, user_id: str, sku: str) -> bool:
        seen = self.__load_or_create_seen(user_id)
        return seen.is_added(sku)

    # ------------------------------------------------------------------------------------------------------------------

    def seen_storage(self, user_id: str) -> Seen:
        seen = self.__load_or_create_seen(user_id)
        return seen

# ----------------------------------------------------------------------------------------------------------------------


class SeenAppService(_SeenAppService):
    def __init__(self):
        from chalicelib.libs.seen.storage import SeenStorageImplementation
        super().__init__(
            SeenStorageImplementation()
        )


# ----------------------------------------------------------------------------------------------------------------------
