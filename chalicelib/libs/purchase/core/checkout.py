from typing import Optional, Tuple
from chalicelib.extensions import *
from .customer import CustomerInterface
from .values import DeliveryAddress
from .product import ProductInterface
from .values import Id, EventCode, SimpleSku, Qty, Cost, Percentage


# ----------------------------------------------------------------------------------------------------------------------


class _CheckoutItem(object):
    def __init__(self, product: ProductInterface, qty: Qty):
        if not isinstance(product, ProductInterface):
            raise ArgumentTypeException(self.__init__, 'product', ProductInterface)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.__init__, 'qty', qty)
        elif qty.value < 1:
            raise ArgumentValueException('{} expects qty > 0'.format(self.__init__.__qualname__))

        self.__product = product
        self.__qty = qty

    @property
    def event_code(self) -> EventCode:
        return self.__product.event_code

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
    def original_cost(self) -> Cost:
        return Cost(self.__qty.value * self.__product.original_price.value)

    @property
    def current_cost(self) -> Cost:
        return Cost(self.__qty.value * self.__product.current_price.value)

    @property
    def qty(self) -> Qty:
        return self.__qty

    @property
    def is_added_over_limit(self) -> bool:
        return self.__qty.value > self.__product.qty_available.value

# ----------------------------------------------------------------------------------------------------------------------


class Checkout(object):
    class Item(_CheckoutItem): pass

    def __init__(
        self,
        customer: CustomerInterface,
        checkout_items: Tuple['Checkout.Item'],
        delivery_cost: Cost,
        vat_percent: Percentage
    ):
        if not isinstance(customer, CustomerInterface):
            raise ArgumentTypeException(self.__init__, 'customer', customer)

        if sum([not isinstance(checkout_item, Checkout.Item) for checkout_item in checkout_items]) > 0:
            raise TypeError('{0} expects array of {1} in {2}, but {3} is given!'.format(
                self.__init__.__qualname__,
                Checkout.Item.__qualname__,
                'checkout_items',
                str(checkout_items)
            ))

        if not isinstance(delivery_cost, Cost):
            raise ArgumentTypeException(self.__init__, 'delivery_cost', delivery_cost)

        if not isinstance(vat_percent, Percentage):
            raise ArgumentTypeException(self.__init__, 'vat_percent', vat_percent)

        self.__customer = customer
        self.__checkout_items = [checkout_item for checkout_item in checkout_items]
        self.__delivery_address = None
        self.__delivery_cost = delivery_cost
        self.__vat_percent = vat_percent

        # Available credits amount must be set on checkout initialization, because
        # amount can be changed by other process during current checkout (other browser, cash-out, etc.).
        # Currently this is unavailable, because we always have only one checkout process for user (used user.id)
        # and we don't use other credits, expect f-bucks, which can be only increased in other process,
        # but we can't trust this, because we should not know about these things on this layer.
        # We should reserve credit amounts at the start of checkout process.
        # This will protect us from multiple usage of the same credit amounts.
        # Another way is to load fresh credits amount every time, but in this case
        # user can see different amounts before and after payment operation. And looks like other strange things
        # can happen in this case. So the best way is reservation.
        # @todo : implement reservation of credit amounts ???

        # @todo : raw data usage. should be used customer.credits or so ???
        from chalicelib.settings import settings
        from chalicelib.libs.core.elastic import Elastic
        available_credits_amount = Cost((Elastic(
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT
        ).get_data(customer.customer_id.value) or {'amount': 0}).get('amount') or 0)

        self.__available_credits_amount = available_credits_amount
        self.__is_credits_in_use = False

        # @todo : remember delivery cost on initialization ???
        # otherwise displayed and final payments can be different, if delivery fee will be changed between requests.

    @property
    def customer_id(self) -> Id:
        return self.__customer.customer_id

    @property
    def checkout_items(self) -> Tuple['Checkout.Item']:
        return tuple(self.__checkout_items)

    @property
    def original_subtotal(self) -> Cost:
        return Cost(sum([checkout_item.original_cost.value for checkout_item in self.__checkout_items]))

    @property
    def current_subtotal(self) -> Cost:
        return Cost(sum([checkout_item.current_cost.value for checkout_item in self.__checkout_items]))

    @property
    def current_subtotal_vat_amount(self) -> Cost:
        per_percent = self.current_subtotal.value / (100 + self.__vat_percent.value)
        return Cost(per_percent * self.__vat_percent.value)

    @property
    def delivery_address(self) -> Optional[DeliveryAddress]:
        return self.__delivery_address

    @delivery_address.setter
    def delivery_address(self, delivery_address: DeliveryAddress) -> None:
        if not isinstance(delivery_address, DeliveryAddress):
            raise ArgumentTypeException(self.delivery_address, 'delivery_address', delivery_address)

        self.__delivery_address = delivery_address

    @property
    def delivery_cost(self) -> Cost:
        return self.__delivery_cost

    @property
    def vat_percent(self) -> Percentage:
        return self.__vat_percent

    @property
    def __total_cost(self) -> Cost:
        return self.current_subtotal + self.delivery_cost

    @property
    def credits_amount_available(self) -> Cost:
        return self.__available_credits_amount

    @property
    def is_credits_in_use(self) -> bool:
        return self.__is_credits_in_use

    def use_credits(self) -> None:
        if self.is_credits_in_use:
            raise ApplicationLogicException('Credits already in use!')

        if self.credits_amount_available.value == 0:
            raise ApplicationLogicException('There are no Available Credits!')

        self.__is_credits_in_use = True

    def unuse_credits(self) -> None:
        if not self.is_credits_in_use:
            raise ApplicationLogicException('Credits already is not in use!')

        self.__is_credits_in_use = False

    @property
    def credits_amount_in_use(self) -> Cost:
        if not self.is_credits_in_use:
            return Cost(0)

        return Cost(min(self.__total_cost.value, self.__available_credits_amount.value))

    @property
    def total_due(self) -> Cost:
        return self.__total_cost - self.credits_amount_in_use


# ----------------------------------------------------------------------------------------------------------------------


class CheckoutStorageInterface(object):
    def save(self, checkout: Checkout) -> None:
        raise NotImplementedError()

    def load(self, customer_id: Id) -> Optional[Checkout]:
        raise NotImplementedError()

    def remove(self, customer_id: Id) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

