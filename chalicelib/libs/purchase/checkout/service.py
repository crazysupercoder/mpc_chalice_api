from chalicelib.extensions import *
from chalicelib.libs.purchase.core import \
    CartStorageInterface, CustomerStorageInterface, \
    Checkout, CheckoutStorageInterface, \
    Id, DeliveryAddress, Cost, Percentage
from chalicelib.libs.purchase.settings import PurchaseSettings


class _CheckoutAppService(object):
    def __init__(
        self,
        cart_storage: CartStorageInterface,
        customer_storage: CustomerStorageInterface,
        checkout_storage: CheckoutStorageInterface
    ):
        if not isinstance(cart_storage, CartStorageInterface):
            raise ArgumentTypeException(self.__init__, 'cart_storage', cart_storage)

        if not isinstance(customer_storage, CustomerStorageInterface):
            raise ArgumentTypeException(self.__init__, 'customer_storage', customer_storage)

        if not isinstance(checkout_storage, CheckoutStorageInterface):
            raise ArgumentTypeException(self.__init__, 'checkout_storage', checkout_storage)

        self.__cart_storage = cart_storage
        self.__customer_storage = customer_storage
        self.__checkout_storage = checkout_storage

        # @todo : create interface, get from arguments
        self.__purchase_settings = PurchaseSettings()

    # ------------------------------------------------------------------------------------------------------------------

    def init(self, customer_id: str, cart_id: str) -> None:
        customer_id = Id(customer_id)
        customer = self.__customer_storage.get_by_id(customer_id)
        if not customer:
            raise ApplicationLogicException('Customer does not exist!')

        cart_id = Id(cart_id)
        cart = self.__cart_storage.get_by_id(cart_id)
        if not cart:
            raise ApplicationLogicException('Cart does not exist!')
        elif cart.is_empty:
            raise ApplicationLogicException('Cart is empty!')
        elif cart.has_products_added_over_limit:
            raise ApplicationLogicException('Cart has Products added over limit!')

        vat_percent = Percentage(self.__purchase_settings.vat)
        delivery_cost = Cost(self.__purchase_settings.fee)
        checkout_items = [Checkout.Item(cart_item.product, cart_item.qty) for cart_item in cart.items]
        checkout = Checkout(
            customer,
            tuple(checkout_items),
            delivery_cost,
            vat_percent
        )

        self.__checkout_storage.save(checkout)

    # ------------------------------------------------------------------------------------------------------------------

    def remove(self, checkout_id: str) -> None:
        checkout_id = Id(checkout_id)
        checkout = self.__checkout_storage.load(checkout_id)
        if not checkout:
            raise ApplicationLogicException('Checkout does not exist!')

        self.__checkout_storage.remove(checkout_id)

    # ------------------------------------------------------------------------------------------------------------------

    def set_delivery_address(self, checkout_id: str, customer_address_hash: str) -> None:
        checkout_id = customer_id = Id(checkout_id)

        # @todo : use Id or create Hash object-value
        if not isinstance(customer_address_hash, str):
            raise ArgumentTypeException(self.set_delivery_address, 'customer_address_hash', customer_address_hash)
        elif not str(customer_address_hash).strip():
            raise ArgumentCannotBeEmptyException(self.set_delivery_address, 'customer_address_hash')

        checkout = self.__checkout_storage.load(checkout_id)
        if not checkout:
            raise ApplicationLogicException('Checkout does not exist!')

        customer = self.__customer_storage.get_by_id(customer_id)
        if not customer:
            raise ApplicationLogicException('Customer does not exist!')

        for delivery_address in customer.delivery_addresses:
            if delivery_address.address_hash == customer_address_hash:
                customer_delivery_address = delivery_address
                break
        else:
            raise ApplicationLogicException('Customer Delivery Address does not exist!')

        delivery_address = DeliveryAddress(
            customer_delivery_address.recipient_name,
            customer_delivery_address.phone_number,
            customer_delivery_address.street_address,
            customer_delivery_address.suburb,
            customer_delivery_address.city,
            customer_delivery_address.province,
            customer_delivery_address.complex_building,
            customer_delivery_address.postal_code,
            customer_delivery_address.business_name,
            customer_delivery_address.special_instructions
        )

        checkout.delivery_address = delivery_address
        self.__checkout_storage.save(checkout)

# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CheckoutAppService(_CheckoutAppService):
    def __init__(self):
        from chalicelib.libs.purchase.cart.storage import CartStorageImplementation
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
        super().__init__(
            CartStorageImplementation(),
            CustomerStorageImplementation(),
            CheckoutStorageImplementation()
        )

# ----------------------------------------------------------------------------------------------------------------------

