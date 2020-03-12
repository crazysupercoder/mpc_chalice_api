import math
from chalicelib.extensions import *
from .values import Cost
from .checkout import Checkout
from .product import ProductStorageInterface
from .order import Order, OrderStorageInterface
from .dtd import DtdCalculatorInterface
from .customer import CustomerStorageInterface


class PurchaseService(object):
    def __init__(
        self,
        order_storage: OrderStorageInterface,
        product_storage: ProductStorageInterface,
        dtd_calculator: DtdCalculatorInterface,
        customer_storage: CustomerStorageInterface
    ):
        if not isinstance(order_storage, OrderStorageInterface):
            raise ArgumentTypeException(self.__init__, 'order_storage', order_storage)
        if not isinstance(product_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'product_storage', product_storage)
        if not isinstance(dtd_calculator, DtdCalculatorInterface):
            raise ArgumentTypeException(self.__init__, 'dtd_calculator', dtd_calculator)
        if not isinstance(customer_storage, CustomerStorageInterface):
            raise ArgumentTypeException(self.__init__, 'customer_storage', customer_storage)

        self.__order_storage = order_storage
        self.__product_storage = product_storage
        self.__dtd_calculator = dtd_calculator
        self.__customer_storage = customer_storage

    # ------------------------------------------------------------------------------------------------------------------

    def purchase(self, checkout: Checkout) -> Order:
        order = self.__create_order(checkout)
        self.__order_storage.save(order)

        for order_item in order.items:
            product = self.__product_storage.load(order_item.simple_sku)
            product.sell_qty(order_item.qty_ordered)
            self.__product_storage.update(product)

        return order

    # ------------------------------------------------------------------------------------------------------------------

    def __create_order(self, checkout: Checkout) -> Order:
        import datetime
        now = datetime.datetime.now()
        order_number = Order.Number(
            # @todo : creator ???
            # Creating an order is not a very frequent operation.
            # "03" - MPC store id in order number. Last 6 digits must be unique in day.
            # It's hard to match in all of 6 microseconds digits twice, so, looks like, it's a best way.
            now.strftime("%y%m%d{}{}").format('03', now.strftime('%f'))
        )
        customer_id = checkout.customer_id
        delivery_cost = checkout.delivery_cost
        delivery_address = checkout.delivery_address
        if not delivery_address:
            raise ApplicationLogicException('Delivery Address is not set!')

        credits_spent = checkout.credits_amount_in_use

        customer = self.__customer_storage.get_by_id(customer_id)

        order_items = []
        for checkout_item in checkout.checkout_items:
            if checkout_item.is_added_over_limit:
                raise ApplicationLogicException('Unable to purchase Products added over limit!')

            dtd = self.__dtd_calculator.calculate(checkout_item.simple_sku, checkout_item.qty)

            fbucks_amount = checkout_item.product_current_price.value * customer.tier.credit_back_percent.value / 100
            fbucks_amount = math.ceil(fbucks_amount)
            fbucks_amount = Cost(fbucks_amount)

            order_item = Order.Item(
                checkout_item.event_code,
                checkout_item.simple_sku,
                checkout_item.product_original_price,
                checkout_item.product_current_price,
                dtd,
                checkout_item.qty,
                fbucks_amount
            )
            order_items.append(order_item)

        if not order_items:
            raise ApplicationLogicException('Unable to order no Products!')

        order_items = tuple(order_items)

        order = Order(
            order_number,
            customer_id,
            order_items,
            delivery_address,
            delivery_cost,
            checkout.vat_percent,
            credits_spent
        )

        return order

