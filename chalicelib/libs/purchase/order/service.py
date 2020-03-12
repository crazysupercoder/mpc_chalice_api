from chalicelib.extensions import *
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.mailer import MailerInterface
from chalicelib.libs.core.sqs_sender import SqsSenderInterface
from chalicelib.libs.purchase.core import \
    Id, \
    ProductStorageInterface, \
    Checkout, CheckoutStorageInterface, \
    CustomerStorageInterface,\
    Order, OrderStorageInterface, \
    DtdCalculatorInterface, \
    PurchaseService
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent


# ----------------------------------------------------------------------------------------------------------------------


class _OrderAppService(object):
    def __init__(
        self,
        product_storage: ProductStorageInterface,
        checkout_storage: CheckoutStorageInterface,
        customer_storage: CustomerStorageInterface,
        dtd_calculator: DtdCalculatorInterface,
        order_storage: OrderStorageInterface,
        sqs_sender: SqsSenderInterface,
        mailer: MailerInterface,
        logger: Logger
    ):
        if not isinstance(product_storage, ProductStorageInterface):
            raise ArgumentTypeException(self.__init__, 'product_storage', product_storage)
        if not isinstance(checkout_storage, CheckoutStorageInterface):
            raise ArgumentTypeException(self.__init__, 'checkout_storage', checkout_storage)
        if not isinstance(customer_storage, CustomerStorageInterface):
            raise ArgumentTypeException(self.__init__, 'customer_storage', customer_storage)
        if not isinstance(dtd_calculator, DtdCalculatorInterface):
            raise ArgumentTypeException(self.__init__, 'dtd_calculator', dtd_calculator)
        if not isinstance(order_storage, OrderStorageInterface):
            raise ArgumentTypeException(self.__init__, 'order_storage', order_storage)
        if not isinstance(sqs_sender, SqsSenderInterface):
            raise ArgumentTypeException(self.__init__, 'sqs_sender', sqs_sender)
        if not isinstance(mailer, MailerInterface):
            raise ArgumentTypeException(self.__init__, 'mailer', mailer)
        if not isinstance(logger, Logger):
            raise ArgumentTypeException(self.__init__, 'logger', logger)

        self.__product_storage = product_storage
        self.__order_storage = order_storage
        self.__checkout_storage = checkout_storage
        self.__customer_storage = customer_storage
        self.__dtd_calculator = dtd_calculator
        self.__sqs_sender = sqs_sender
        self.__mailer = mailer
        self.__logger = logger

        self.__purchase_service = PurchaseService(
            self.__order_storage,
            self.__product_storage,
            self.__dtd_calculator,
            self.__customer_storage
        )

    # ------------------------------------------------------------------------------------------------------------------

    def get_waiting_for_payment_by_checkout_or_checkout_new(self, user_id: str) -> Order:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('Create/Get Last order for User #{} : {}'.format(user_id, text))

        # @todo : refactoring...

        # For some reasons order payments can fail (closing tabs during payment process, errors, ...).
        # This forces us to start process again, but in this case new order will be created.
        # Theoretically this is still correct, but we get many "failed" orders, which are cancelled by timeout.
        # Better way is using just now created orders, which can be detected by current checkout data.
        # @TODO : SHOULD BE 2 SEPARATED ACTIONS: CREATE ORDER AND DO PAYMENT. Current flow is not good.

        customer_id = Id(user_id)
        checkout = self.__checkout_storage.load(customer_id)
        if not checkout:
            raise ApplicationLogicException('Checkout does not exist!')

        # find or create new order
        order = None
        existed_orders = self.__order_storage.get_all_for_customer(checkout.customer_id)
        for existed_order in existed_orders:
            # this is not good method, but no ideas for now
            # EFT-payment orders are still in waiting_for_payment status after checkout complete -
            # this is not usual for other payment types. But each payment process fill payment_method property,
            # so we can separate "existed not paid uncompleted" orders from "existed not paid completed" orders.
            is_waiting_for_payment = existed_order.status.value == Order.Status.AWAITING_PAYMENT
            is_not_set_payment = existed_order.payment_method is None
            if is_waiting_for_payment and is_not_set_payment:
                if self.__is_order_matched_checkout(existed_order, checkout):
                    order = existed_order
                    __log_flow('Found Existed Order #{} matched with Checkout'.format(existed_order.number.value))
                    break

        if not order:
            # create order
            order = self.__purchase_service.purchase(checkout)
            __log_flow('Created New Order #{}'.format(order.number.value))

            # send to portal
            # Theoretically we can redo it again, if some error occurs during process.
            try:
                __log_flow('New Order #{} SQS Sending...'.format(order.number.value))
                event = OrderChangeSqsSenderEvent(order)
                self.__sqs_sender.send(event)
                __log_flow('New Order #{} SQS Sent!'.format(order.number.value))
            except BaseException as e:
                self.__logger.log_exception(e)
                __log_flow('New Order #{} SQS NOT Sent because of Error: {}!'.format(order.number.value, str(e)))

        return order

    def __is_order_matched_checkout(self, order: Order, checkout: Checkout):
        import json
        import hashlib

        # @todo : coupons

        checkout_hash = hashlib.md5(json.dumps({
            'items': [{
                'sku': item.simple_sku.value,
                'qty': item.qty.value,
            } for item in checkout.checkout_items],
            'delivery_address': checkout.delivery_address.address_hash,
            'total_cost': checkout.total_due.value,
        }).encode('utf-8')).hexdigest()

        order_hash = hashlib.md5(json.dumps({
            'items': [{
                'sku': item.simple_sku.value,
                'qty': item.qty_ordered.value,
            } for item in order.items],
            'delivery_address': order.delivery_address.address_hash,
            'total_cost': order.total_current_cost_ordered.value
        }).encode('utf-8')).hexdigest()

        return checkout_hash == order_hash


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class OrderAppService(_OrderAppService):
    def __init__(self):
        from chalicelib.libs.purchase.product.storage import ProductStorageImplementation
        from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        from chalicelib.libs.purchase.order.dtd_calculator import DtdCalculatorImplementation
        from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
        from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
        from chalicelib.libs.core.mailer import MailerImplementation
        from chalicelib.libs.core.logger import Logger
        super().__init__(
            ProductStorageImplementation(),
            CheckoutStorageImplementation(),
            CustomerStorageImplementation(),
            DtdCalculatorImplementation(),
            OrderStorageImplementation(),
            SqsSenderImplementation(),
            MailerImplementation(),
            Logger()
        )


# ----------------------------------------------------------------------------------------------------------------------
