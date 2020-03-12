import datetime
from typing import Optional, Tuple
from chalicelib.extensions import *
from .values import Id, EventCode, SimpleSku, Qty, Cost, DeliveryAddress, OrderNumber as _OrderNumber, Percentage
from .dtd import Dtd
from .payments import PaymentMethodAbstract


# @todo : refactoring


# ----------------------------------------------------------------------------------------------------------------------


class _Status(object):
    AWAITING_PAYMENT = 'Awaiting Payment'
    PAYMENT_SENT = 'Payment Sent'
    PAYMENT_RECEIVED = 'Payment Received'
    AWAITING_COURIER = 'Awaiting Courier Collection'
    IN_TRANSIT = 'In Transit'
    DELIVERED = 'Delivered'
    CANCELLED = 'Cancelled'
    CLOSED = 'Closed'
    ON_HOLD = 'On Hold'

    __LIST = {
        AWAITING_PAYMENT: 'Awaiting Payment',
        PAYMENT_SENT: 'Payment Sent',
        PAYMENT_RECEIVED: 'Payment Received',
        AWAITING_COURIER: 'Awaiting Courier Collection',
        IN_TRANSIT: 'In Transit',
        DELIVERED: 'Delivered',
        CANCELLED: 'Cancelled',
        CLOSED: 'Closed',
        ON_HOLD: 'On Hold',
    }

    def __init__(self, value: str) -> None:
        known_values = tuple(self.__class__.__LIST.keys())
        if value not in known_values:
            raise ArgumentUnexpectedValueException(value, known_values)

        self.__value = value

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self.value == other.value

    @property
    def value(self) -> str:
        return self.__value

    @property
    def label(self) -> str:
        return self.__class__.__LIST[self.value]


# ----------------------------------------------------------------------------------------------------------------------


class _StatusChangesHistory(object):
    class Change(object):
        def __init__(self, status: 'Order.Status'):
            if not isinstance(status, Order.Status):
                raise ArgumentTypeException(self.__init__, 'status', status)

            self.__status = status
            self.__datetime = datetime.datetime.now()

        @property
        def datetime(self) -> datetime.datetime:
            # datetime objects are already immutable
            return self.__datetime

        @property
        def status(self) -> 'Order.Status':
            return self.__status

    def __init__(self, changes: Tuple['Order.StatusChangesHistory.Change']) -> None:
        self.__changes = []
        for change in changes:
            if not isinstance(change, Order.StatusChangesHistory.Change):
                raise ArgumentTypeException(self.__init__, 'changes', changes)

            self.__changes.append(change)

    def add(self, change: 'Order.StatusChangesHistory.Change') -> None:
        if not isinstance(change, Order.StatusChangesHistory.Change):
            raise ArgumentTypeException(self.add, 'change', change)

        self.__changes.append(change)

    def get_last(self) -> Optional['Order.StatusChangesHistory.Change']:
        return self.__changes[-1] if self.__changes else None

    def get_last_concrete(self, status_value: str) -> Optional['Order.StatusChangesHistory.Change']:
        for i in range(len(self.__changes) - 1, -1, -1):
            if self.__changes[i].status.value == status_value:
                return self.__changes[i]
        else:
            return None

    def get_all(self) -> Tuple['Order.StatusChangesHistory.Change']:
        return tuple(self.__changes)


# ----------------------------------------------------------------------------------------------------------------------


class _Item(object):
    def __init__(
        self,
        event_code: EventCode,
        simple_sku: SimpleSku,
        product_original_price: Cost,
        product_current_price: Cost,
        dtd: Dtd,
        qty_ordered: Qty,
        fbucks_earnings: Cost
    ) -> None:
        if not isinstance(event_code, EventCode):
            raise ArgumentTypeException(self.__init__, 'event_code', event_code)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.__init__, 'simple_sku', simple_sku)

        if not isinstance(product_original_price, Cost):
            raise ArgumentTypeException(self.__init__, 'product_original_price', product_original_price)
        if not isinstance(product_current_price, Cost):
            raise ArgumentTypeException(self.__init__, 'product_current_price', product_current_price)

        if not isinstance(dtd, Dtd):
            raise ArgumentTypeException(self.__init__, 'dtd', dtd)

        if not isinstance(qty_ordered, Qty):
            raise ArgumentTypeException(self.__init__, 'qty_ordered', qty_ordered)
        elif qty_ordered.value < 1:
            raise ArgumentValueException('{} expects qty_ordered > 0'.format(self.__init__.__qualname__))

        if not isinstance(fbucks_earnings, Cost):
            raise ArgumentTypeException(self.__init__, 'fbucks_earnings', fbucks_earnings)

        self.__event_code = event_code
        self.__simple_sku = simple_sku
        self.__product_original_price = product_original_price
        self.__product_current_price = product_current_price
        self.__dtd = dtd
        self.__qty_ordered = qty_ordered
        self.__qty_return_requested = Qty(0)
        self.__qty_return_returned = Qty(0)
        self.__qty_cancelled_before_payment = Qty(0)
        self.__qty_cancelled_after_payment_requested = Qty(0)
        self.__qty_cancelled_after_payment_cancelled = Qty(0)
        self.__qty_refunded = Qty(0)
        self.__qty_modified_at = datetime.datetime.now()
        self.__fbucks_earnings = fbucks_earnings

    @property
    def event_code(self) -> EventCode:
        return self.__event_code

    @property
    def simple_sku(self) -> SimpleSku:
        return self.__simple_sku

    @property
    def product_original_price(self) -> Cost:
        return self.__product_original_price

    @property
    def product_current_price(self) -> Cost:
        return self.__product_current_price

    @property
    def dtd(self) -> Dtd:
        return self.__dtd

    @property
    def fbucks_earnings(self) -> Cost:
        return self.__fbucks_earnings

    @property
    def qty_ordered(self) -> Qty:
        return self.__qty_ordered

    @property
    def qty_return_requested(self) -> Qty:
        return self.__qty_return_requested

    @property
    def qty_return_returned(self) -> Qty:
        return self.__qty_return_returned

    @property
    def qty_cancelled_before_payment(self) -> Qty:
        return self.__qty_cancelled_before_payment

    @property
    def qty_cancelled_after_payment_requested(self) -> Qty:
        return self.__qty_cancelled_after_payment_requested

    @property
    def qty_cancelled_after_payment_cancelled(self) -> Qty:
        return self.__qty_cancelled_after_payment_cancelled

    @property
    def qty_processable(self) -> Qty:
        return (
            self.__qty_ordered
            - self.__qty_cancelled_before_payment
            - self.__qty_cancelled_after_payment_requested
            - self.__qty_cancelled_after_payment_cancelled
            - self.__qty_return_requested
            - self.__qty_return_returned
        )

    @property
    def qty_requested(self) -> Qty:
        return self.__qty_return_requested + self.__qty_cancelled_after_payment_requested

    @property
    def qty_refunded(self) -> Qty:
        return self.__qty_refunded

    @property
    def qty_modified_at(self) -> datetime.datetime:
        return self.__qty_modified_at

    def get_refund_cost(self, qty: Qty) -> Cost:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.get_refund_cost, 'qty', qty)

        return Cost(qty.value * self.total_current_cost_ordered.value / self.qty_ordered.value)

    @property
    def total_original_cost_ordered(self) -> Cost:
        return Cost(self.__qty_ordered.value * self.__product_original_price.value)

    @property
    def total_current_cost_ordered(self) -> Cost:
        return Cost(self.__qty_ordered.value * self.__product_current_price.value)

    @property
    def total_current_cost_paid(self) -> Cost:
        qty_paid = self.__qty_ordered.value - self.__qty_cancelled_before_payment.value
        return Cost(qty_paid * self.__product_current_price.value)

    def request_return(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.request_return, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.request_return.__qualname__
            ))

        available_qty = self.qty_processable
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Request Return {} qty, only {} is available!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_return_requested += qty
        self.__qty_modified_at = datetime.datetime.now()

    def decline_return(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.decline_return, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.decline_return.__qualname__
            ))

        available_qty = self.__qty_return_requested
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Decline Return {} qty, only {} is available!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_return_requested -= qty
        self.__qty_modified_at = datetime.datetime.now()

    def reject_return(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.reject_return, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.reject_return.__qualname__
            ))

        available_qty = self.__qty_return_requested
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Reject Return {} qty, only {} is available!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_return_requested -= qty
        self.__qty_modified_at = datetime.datetime.now()

    def close_return(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.close_return, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.close_return.__qualname__
            ))

        available_qty = self.__qty_return_requested
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Close Return {} qty, only {} is requested!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_return_requested -= qty
        self.__qty_return_returned += qty
        self.__qty_modified_at = datetime.datetime.now()

    def cancel_before_payment(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.cancel_before_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.cancel_before_payment.__qualname__
            ))

        available_qty = self.qty_processable
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Cancel {} qty, only {} is available!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_cancelled_before_payment += qty
        self.__qty_modified_at = datetime.datetime.now()

    def request_cancellation_after_payment(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.request_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.request_cancellation_after_payment.__qualname__
            ))

        available_qty = self.qty_processable
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Request Cancellation of {} qty, only {} is available!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_cancelled_after_payment_requested += qty
        self.__qty_modified_at = datetime.datetime.now()

    def approve_cancellation_after_payment(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.approve_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.approve_cancellation_after_payment.__qualname__
            ))

        available_qty = self.__qty_cancelled_after_payment_requested
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Approve Cancellation of {} qty, only {} is requested!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_cancelled_after_payment_requested -= qty
        self.__qty_cancelled_after_payment_cancelled += qty
        self.__qty_modified_at = datetime.datetime.now()

    def decline_cancellation_after_payment(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.decline_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.decline_cancellation_after_payment.__qualname__
            ))

        available_qty = self.__qty_cancelled_after_payment_requested
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Decline Cancellation of {} qty, only {} is requested!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_cancelled_after_payment_requested -= qty
        self.__qty_modified_at = datetime.datetime.now()

    @property
    def __refundable_qty(self) -> Qty:
        return (
            self.__qty_return_returned
            + self.__qty_cancelled_after_payment_cancelled
            - self.__qty_refunded
        )

    @property
    def is_refundable(self) -> bool:
        return self.__refundable_qty.value > 0

    def refund(self, qty: Qty) -> None:
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.refund, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.refund.__qualname__
            ))

        if not self.is_refundable:
            raise ApplicationLogicException('Item "{}" is not Refundable!'.format(self.simple_sku.value))

        available_qty = self.__refundable_qty
        if available_qty < qty:
            raise ApplicationLogicException('Unable to Refund {} qty, only {} is requested!'.format(
                qty.value,
                available_qty.value
            ))

        self.__qty_refunded += qty
        self.__qty_modified_at = datetime.datetime.now()


# ----------------------------------------------------------------------------------------------------------------------


class Order(object):
    Number = _OrderNumber   # not really inner class, but it is final, and this is just for "Order.Number" usage
    class Status(_Status): pass
    class StatusChangesHistory(_StatusChangesHistory): pass
    class PaymentMethodAbstract(PaymentMethodAbstract): pass
    class Item(_Item): pass

    def __init__(
        self,
        order_number: 'Order.Number',
        customer_id: Id,
        items: Tuple['Order.Item'],
        delivery_address: DeliveryAddress,
        delivery_cost: Cost,
        vat_percent: Percentage,
        credits_spent: Cost
    ) -> None:
        if not isinstance(order_number, Order.Number):
            raise ArgumentTypeException(self.__init__, 'order_number', order_number)

        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.__init__, 'customer_id', customer_id)

        if not isinstance(items, tuple):
            raise ArgumentTypeException(self.__init__, 'items', items)
        elif sum([not isinstance(i, Order.Item) for i in items]) > 0:
            raise TypeError('{0} expects tuple of {1}, {2} given!'.format(
                self.__init__.__qualname__,
                Order.Item.__qualname__,
                str(items)
            ))
        elif not items:
            raise ArgumentValueException('{} expects at least one order item!'.format(self.__init__.__qualname__))

        if not isinstance(delivery_address, DeliveryAddress):
            raise ArgumentTypeException(self.__init__, 'delivery_address', delivery_address)

        if not isinstance(delivery_cost, Cost):
            raise ArgumentTypeException(self.__init__, 'delivery_cost', delivery_cost)

        if not isinstance(vat_percent, Percentage):
            raise ArgumentTypeException(self.__init__, 'vat_percent', vat_percent)

        if not isinstance(credits_spent, Cost):
            raise ArgumentTypeException(self.__init__, 'credits_spent', credits_spent)

        self.__order_number = order_number
        self.__customer_id = customer_id
        self.__items = items
        self.__delivery_address = delivery_address
        self.__delivery_cost = delivery_cost
        self.__vat_percent = vat_percent
        self.__credits_spent = credits_spent
        self.__payment_method = None
        self.__status_history = Order.StatusChangesHistory(tuple([
            Order.StatusChangesHistory.Change(Order.Status(Order.Status.AWAITING_PAYMENT))
        ]))

    def __change_status(self, new_status: 'Order.Status') -> None:
        if not isinstance(new_status, Order.Status):
            raise ArgumentTypeException(self.status, 'new_status', new_status)

        # Attention!
        # - Delivered order can only be returned
        # - Delivered order cannot be cancelled
        # - Unable to cancel qty between "Payment Sent" and "Payment Received" statuses

        is_all_qty_processed = self.__is_all_qty_processed
        qty_ordered = sum([item.qty_ordered.value for item in self.__items])
        qty_cancelled_before_payment = sum([
            item.qty_cancelled_before_payment.value
            for item in self.__items
        ])
        qty_cancelled_after_payment_cancelled = sum([
            item.qty_cancelled_after_payment_cancelled.value
            for item in self.__items
        ])
        qty_return_returned = sum([item.qty_return_returned.value for item in self.__items])
        qty_refunded = sum([item.qty_refunded.value for item in self.__items])

        def __always():
            return True

        def __is_customer_rs_staff():
            # @todo : set customer in constructor, storage instead of id
            # @todo : customer.is_rs_staff and email domain to settings or so ?
            from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
            customer = CustomerStorageImplementation().get_by_id(self.customer_id)
            customer_is_rs_staff = customer.email.value.split('@')[-1] == 'runwaysale.co.za'
            return self.__delivery_address.postal_code == '7777' or customer_is_rs_staff

        def __was_only_in_statuses(statuses):
            for _change in self.__status_history.get_all():
                if _change.status.value not in statuses:
                    return False

            return True

        changes_map = {
            Order.Status.AWAITING_PAYMENT: {
                Order.Status.ON_HOLD: __always,
                Order.Status.PAYMENT_SENT: __always,
                Order.Status.CANCELLED: lambda: is_all_qty_processed and qty_ordered == qty_cancelled_before_payment,
            },
            Order.Status.PAYMENT_SENT: {
                Order.Status.ON_HOLD: __always,
                Order.Status.PAYMENT_RECEIVED: __always,
                Order.Status.CLOSED: __always,  # e.g. declined payment @todo : refactoring payment ?
            },
            Order.Status.PAYMENT_RECEIVED: {
                Order.Status.ON_HOLD: __always,
                Order.Status.AWAITING_COURIER: __always,
                Order.Status.DELIVERED: __is_customer_rs_staff,
                Order.Status.CANCELLED: lambda: is_all_qty_processed and qty_cancelled_after_payment_cancelled > 0,
            },
            Order.Status.ON_HOLD: {
                # only return back to itself - next statuses expect defined payment method
                Order.Status.AWAITING_PAYMENT: lambda: __was_only_in_statuses((
                    Order.Status.AWAITING_PAYMENT,
                    Order.Status.ON_HOLD,
                )),

                # there is "Un-Hold" button, which turns order to previous status.
                Order.Status.PAYMENT_SENT: lambda: self.__was_payment_sent and not self.was_paid,

                Order.Status.PAYMENT_RECEIVED: lambda: (
                    self.__was_payment_sent     # good line, but through on-hold
                    or self.was_paid            # return back to itself
                    # looks like, there is no sense of going back,
                    # but let it be just in case (there is nothing bad)
                    or self.__was_awaiting_courier
                ),

                Order.Status.AWAITING_COURIER: lambda: (
                    self.was_paid                   # good line, but through on-hold
                    or self.__was_awaiting_courier  # return back to itself
                ),

                Order.Status.CANCELLED: lambda: (
                    # cancellation is enabled for on-hold orders
                    (
                        # before payment
                        not self.__was_payment_sent
                        and is_all_qty_processed and qty_cancelled_before_payment == qty_ordered
                    ) or (
                        # after payment
                        self.was_paid
                        and is_all_qty_processed and qty_cancelled_after_payment_cancelled > 0
                    )
                ),
            },
            Order.Status.AWAITING_COURIER: {
                Order.Status.ON_HOLD: __always,
                Order.Status.IN_TRANSIT: __always,
                Order.Status.CANCELLED: lambda: is_all_qty_processed and qty_cancelled_after_payment_cancelled > 0,
            },
            Order.Status.IN_TRANSIT: {
                Order.Status.DELIVERED: __always,
            },
            Order.Status.DELIVERED: {
                Order.Status.CLOSED: lambda: (
                    # the last qty was refunded as returned
                    is_all_qty_processed
                    and qty_return_returned > 0
                    and qty_return_returned + qty_cancelled_after_payment_cancelled == qty_refunded
                ),
            },
            Order.Status.CANCELLED: {
                Order.Status.CLOSED: lambda: (
                    # all refunded as cancelled after payment
                    self.was_paid
                    and is_all_qty_processed
                    and qty_cancelled_after_payment_cancelled == qty_refunded
                ),
            },
            Order.Status.CLOSED: {},
        }
        if changes_map.get(self.status.value, None) is None:
            raise Exception('{} does not know, how to work with "{}" for Order #{}!'.format(
                self.__change_status,
                self.status.value,
                self.number.value
            ))
        elif (
            new_status.value not in changes_map[self.status.value].keys()
            or not changes_map[self.status.value][new_status.value]()
        ):
            raise ApplicationLogicException('Unable to change Order Status from "{}" to "{}" for Order #{}!'.format(
                self.status.label,
                new_status.label,
                self.number.value
            ))

        change = Order.StatusChangesHistory.Change(new_status)
        self.__status_history.add(change)

    @property
    def number(self) -> 'Order.Number':
        return self.__order_number

    @property
    def customer_id(self) -> Id:
        return self.__customer_id

    @property
    def items(self) -> Tuple['Order.Item']:
        return self.__items

    @property
    def delivery_address(self) -> DeliveryAddress:
        return self.__delivery_address

    @property
    def subtotal_original_cost_ordered(self) -> Cost:
        return Cost(sum([order_item.total_original_cost_ordered.value for order_item in self.__items]))

    @property
    def subtotal_current_cost_ordered(self) -> Cost:
        return Cost(sum([order_item.total_current_cost_ordered.value for order_item in self.__items]))

    @property
    def subtotal_vat_amount(self) -> Cost:
        per_percent = self.subtotal_current_cost_ordered.value / (100 + self.__vat_percent.value)
        return Cost(per_percent * self.__vat_percent.value)

    @property
    def delivery_cost(self) -> Cost:
        return self.__delivery_cost

    @property
    def vat_percent(self) -> Percentage:
        return self.__vat_percent

    @property
    def credit_spent_amount(self) -> Cost:
        return self.__credits_spent

    @property
    def total_original_cost_ordered(self) -> Cost:
        return Cost(
            self.subtotal_original_cost_ordered.value
            + self.delivery_cost.value
            - self.credit_spent_amount.value
        )

    @property
    def total_current_cost_ordered(self) -> Cost:
        return Cost(
            self.subtotal_current_cost_ordered.value
            + self.delivery_cost.value
            - self.credit_spent_amount.value
        )

    @property
    def total_current_cost_paid(self) -> Cost:
        return Cost(
            sum([order_item.total_current_cost_paid.value for order_item in self.__items])
            + self.delivery_cost.value
            - self.credit_spent_amount.value
        )

    @property
    def total_refunded_cost(self) -> Cost:
        return Cost(
            sum([
                order_item.product_current_price.value * order_item.qty_refunded.value
                for order_item in self.__items
            ])
        )

    @property
    def total_fbucks_earnings(self) -> Cost:
        return Cost(sum([order_item.fbucks_earnings.value for order_item in self.__items]))

    @property
    def payment_method(self) -> Optional['Order.PaymentMethodAbstract']:
        return self.__payment_method

    @payment_method.setter
    def payment_method(self, payment_method: 'Order.PaymentMethodAbstract') -> None:
        if not isinstance(payment_method, Order.PaymentMethodAbstract):
            raise ArgumentTypeException(self.payment_method, 'payment_method', payment_method)
        elif self.__payment_method is not None:
            raise ApplicationLogicException('Payment method is already set!')
        elif self.status.value != Order.Status.AWAITING_PAYMENT:
            raise ApplicationLogicException('Unable to set Payment Method to not {} order!'.format(
                Order.Status(Order.Status.AWAITING_PAYMENT).label
            ))

        self.__payment_method = payment_method

    @property
    def status(self) -> 'Order.Status':
        return self.__status_history.get_last().status

    @status.setter
    def status(self, new_status: 'Order.Status') -> None:
        self.__change_status(new_status)

    @property
    def created_at(self) -> datetime.datetime:
        return self.__status_history.get_last_concrete(Order.Status.AWAITING_PAYMENT).datetime

    @property
    def updated_at(self) -> datetime.datetime:
        return max(
            self.__status_history.get_last().datetime,
            max([item.qty_modified_at for item in self.__items])
        )

    @property
    def __delivered_at(self) -> Optional[datetime.datetime]:
        change = self.__status_history.get_last_concrete(Order.Status.DELIVERED)
        return change.datetime if change else None

    @property
    def status_history(self) -> Tuple['Order.StatusChangesHistory.Change']:
        return self.__status_history.get_all()

    @property
    def __was_payment_sent(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.PAYMENT_SENT) is not None

    @property
    def __was_awaiting_courier(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.AWAITING_COURIER) is not None

    @property
    def was_paid(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.PAYMENT_RECEIVED) is not None

    @property
    def was_in_transit(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.IN_TRANSIT) is not None

    @property
    def was_cancelled(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.CANCELLED) is not None

    @property
    def was_closed(self) -> bool:
        return self.__status_history.get_last_concrete(Order.Status.CLOSED) is not None

    @property
    def was_delivered(self) -> bool:
        return self.__delivered_at is not None

    @property
    def is_returnable(self) -> bool:
        return (
            self.was_delivered
            and not self.was_closed
            and self.is_returnable_till and self.is_returnable_till > datetime.datetime.now()
            and sum([order_item.qty_processable.value for order_item in self.__items]) > 0
        )

    @property
    def is_returnable_till(self) -> Optional[datetime.datetime]:
        if not self.__delivered_at:
            return None

        # @todo : refactoring : magic number usage
        # @todo : refactoring : move somewhere ???
        return_window_size = datetime.timedelta(days=14)

        return self.__delivered_at + return_window_size

    def __return_qty_action(self, action: str, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.__return_qty_action, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.__return_qty_action, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.__return_qty_action.__qualname__
            ))

        actions_map = {
            'request': {'method': Order.Item.request_return, 'action_label': 'Request'},
            'decline': {'method': Order.Item.decline_return, 'action_label': 'Decline'},
            'reject': {'method': Order.Item.reject_return, 'action_label': 'Reject'},
            'close': {'method': Order.Item.close_return, 'action_label': 'Close'},
        }
        action_params = actions_map.get(action)
        if not action_params:
            raise ValueError('{} does not know, how to work with {} action!'.format(self.__return_qty_action, action))

        logic_error_message_prefix = 'Unable to {} Return {} qty of {} Product for Order #{}: '.format(
            action_params['action_label'],
            qty.value,
            simple_sku.value,
            self.number.value
        )

        if not self.was_delivered:
            raise ApplicationLogicException(logic_error_message_prefix + 'Order is not Delivered!')

        for order_item in self.__items:
            if order_item.simple_sku == simple_sku:
                try:
                    action_params['method'].__call__(order_item, qty)
                except ApplicationLogicException as e:
                    raise ApplicationLogicException(logic_error_message_prefix + str(e))
                break
        else:
            raise ApplicationLogicException(logic_error_message_prefix + 'Product does not exist!'.format(
                self.number.value,
                simple_sku.value
            ))

    def request_return(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not self.is_returnable:
            raise ApplicationLogicException('Return can not be Requested for Order #{}!'.format(
                self.number.value
            ))

        self.__return_qty_action('request', simple_sku, qty)

    def decline_return(self, simple_sku: SimpleSku, qty: Qty) -> None:
        self.__return_qty_action('decline', simple_sku, qty)

    def reject_return(self, simple_sku: SimpleSku, qty: Qty) -> None:
        self.__return_qty_action('reject', simple_sku, qty)

    def close_return(self, simple_sku: SimpleSku, qty: Qty) -> None:
        self.__return_qty_action('close', simple_sku, qty)

    @property
    def __is_all_qty_processed(self) -> bool:
        """ no processable qty and no requested qty """
        return sum([
            item.qty_processable.value + item.qty_requested.value
            for item in self.__items
        ]) == 0

    @property
    def __is_any_qty_processable(self) -> bool:
        return sum([item.qty_processable.value for item in self.__items]) > 0

    @property
    def is_cancellable(self) -> bool:
        is_payment_sent_but_not_received = (self.__was_payment_sent and not self.was_paid)

        return (
            not self.was_cancelled
            and not self.was_closed
            and not self.was_delivered
            and not self.was_in_transit
            and not is_payment_sent_but_not_received
            and self.__is_any_qty_processable
        )

    def cancel_before_payment(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.cancel_before_payment, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.cancel_before_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.cancel_before_payment.__qualname__
            ))

        if not self.is_cancellable:
            raise ApplicationLogicException('Order #{} is Not Cancellable!'.format(self.number.value))

        if self.__was_payment_sent:
            raise Exception('{} cannot work with Order #{}, because Payment is Sent!'.format(
                self.cancel_before_payment.__qualname__,
                self.number.value
            ))

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.cancel_before_payment(qty)
                break
        else:
            raise ApplicationLogicException('Product "{}" does not exist in Order #{}!'.format(
                simple_sku.value,
                self.number.value
            ))

        if self.__is_all_qty_processed:
            self.__change_status(Order.Status(Order.Status.CANCELLED))

    def request_cancellation_after_payment(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.request_cancellation_after_payment, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.request_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.request_cancellation_after_payment.__qualname__
            ))

        if not self.is_cancellable:
            raise ApplicationLogicException('Order #{} is Not Cancellable!'.format(self.number.value))

        if not self.was_paid:
            raise Exception('{} cannot work with Order #{}, because Payment is Not Sent!'.format(
                self.request_cancellation_after_payment.__qualname__,
                self.number.value
            ))

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.request_cancellation_after_payment(qty)
                break
        else:
            raise ApplicationLogicException('Product "{}" does not exist in Order #{}!'.format(
                simple_sku.value,
                self.number.value
            ))

    def approve_cancellation_after_payment(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.approve_cancellation_after_payment, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.approve_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.approve_cancellation_after_payment.__qualname__
            ))

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.approve_cancellation_after_payment(qty)
                break
        else:
            raise ApplicationLogicException('Product "{}" does not exist in Order #{}!'.format(
                simple_sku.value,
                self.number.value
            ))

        if self.__is_all_qty_processed:
            self.__change_status(Order.Status(Order.Status.CANCELLED))

    def decline_cancellation_after_payment(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.decline_cancellation_after_payment, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.decline_cancellation_after_payment, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(
                self.decline_cancellation_after_payment.__qualname__
            ))

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.decline_cancellation_after_payment(qty)
                break
        else:
            raise ApplicationLogicException('Product "{}" does not exist in Order #{}!'.format(
                simple_sku.value,
                self.number.value
            ))

    @property
    def is_refundable(self) -> bool:
        return sum([item.is_refundable for item in self.__items]) > 0

    def refund(self, simple_sku: SimpleSku, qty: Qty) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.refund, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.refund, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(self.refund.__qualname__))

        if not self.is_refundable:
            raise ApplicationLogicException('Order #{} is not Refundable!'.format(self.number.value))

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.refund(qty)
                break
        else:
            raise ApplicationLogicException('Unable to Refund {} qty - Product {} does not exist in Order #{}'.format(
                qty.value,
                simple_sku.value,
                self.number.value
            ))

        if self.__is_all_qty_processed:
            if not self.was_closed:
                self.__change_status(Order.Status(Order.Status.CLOSED))


# ----------------------------------------------------------------------------------------------------------------------


class OrderStorageInterface(object):
    def save(self, order: Order) -> None:
        raise NotImplementedError()

    def load(self, order_number: Order.Number) -> Optional[Order]:
        raise NotImplementedError()

    def get_all_by_numbers(self, order_numbers: Tuple[Order.Number]) -> Tuple[Order]:
        raise NotImplementedError()

    def get_all_for_customer(self, customer_id: Id) -> Tuple[Order]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

