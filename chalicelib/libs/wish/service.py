from chalicelib.extensions import *
from chalicelib.libs.wish.wish import Wish, WishStorageInterface
from chalicelib.libs.models.mpc.Product import Product

class _WishAppService(object):
    def __init__(
        self,
        wish_storage: WishStorageInterface
    ):

        if not isinstance(wish_storage, WishStorageInterface):
            raise ArgumentTypeException(self.__init__, 'wish_storage', wish_storage)

        self.__wish_storage = wish_storage
        self.__products = Product()

    # ------------------------------------------------------------------------------------------------------------------

    def __load_or_create_wish(self, user_id: str) -> Wish:
        wish = self.__wish_storage.load(user_id)
        wish = wish if wish else Wish(user_id)
        return wish

    # ------------------------------------------------------------------------------------------------------------------

    def add_wish_product(self, user_id: str, sku: str) -> None:

        product = self.__products.get_raw_data(sku, True)
        if not product:
            raise ApplicationLogicException('Product "{0}" does not exist!'.format(sku))

        wish = self.__load_or_create_wish(user_id)
        wish.add_item(product['sku'])
        self.__wish_storage.save(wish)

    # ------------------------------------------------------------------------------------------------------------------

    def remove_wish_product(self, user_id: str, sku: str) -> None:

        wish = self.__load_or_create_wish(user_id)
        wish.remove_item(sku)
        self.__wish_storage.save(wish)

    # ------------------------------------------------------------------------------------------------------------------

    def clear_wish(self, user_id) -> None:
        wish = self.__load_or_create_wish(user_id)
        wish.clear()
        self.__wish_storage.save(wish)


# ----------------------------------------------------------------------------------------------------------------------


class WishAppService(_WishAppService):
    def __init__(self):
        from chalicelib.libs.wish.storage import WishStorageImplementation
        super().__init__(
            WishStorageImplementation()
        )


# ----------------------------------------------------------------------------------------------------------------------
