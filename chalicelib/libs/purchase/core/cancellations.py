import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *
from .values import Id, OrderNumber, SimpleSku, Qty, Description
from .payments import RefundMethodAbstract


class _Number(Id):
    def __init__(self, value: str):
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        if str(int(str(value))) != str(value):
            raise ArgumentValueException('{} expects 13 digits string, but "{}" is given!'.format(
                self.__init__.__qualname__,
                value
            ))

        super().__init__(value)


class _Status(object):
    PENDING_APPROVAL = 'pending_approval'
    APPROVED = 'approved'
    DECLINED = 'declined'

    __LIST = {
        PENDING_APPROVAL: 'Pending Approval',
        APPROVED: 'Approved',
        DECLINED: 'Declined',
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


class _AdditionalComment(Description):
    pass


class _Item(object):
    class Status(_Status): pass

    def __init__(self, simple_sku: SimpleSku, qty: Qty):
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.__init__, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.__init__, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(self.__init__.__qualname__))

        self.__simple_sku = simple_sku
        self.__qty = qty
        self.__status = _Item.Status(_Item.Status.PENDING_APPROVAL)
        self.__processed_at = None

    @property
    def simple_sku(self) -> SimpleSku:
        return self.__simple_sku

    @property
    def qty(self) -> Qty:
        return self.__qty

    @property
    def status(self) -> '_Item.Status':
        return self.__status

    @property
    def processed_at(self) -> Optional[datetime.datetime]:
        return self.__processed_at

    @property
    def is_processable(self) -> bool:
        return self.__processed_at is None

    def approve(self) -> None:
        if not self.is_processable:
            raise ApplicationLogicException('Cancellation Request Item is not Processable!')

        self.__status = _Item.Status(_Item.Status.APPROVED)
        self.__processed_at = datetime.datetime.now()

    def decline(self) -> None:
        if not self.is_processable:
            raise ApplicationLogicException('Cancellation Request Item is not Processable!')

        self.__status = _Item.Status(_Item.Status.DECLINED)
        self.__processed_at = datetime.datetime.now()


# ----------------------------------------------------------------------------------------------------------------------


class CancelRequest(object):
    """
    @todo : rename - only paid orders have requests!
    """

    class Number(_Number): pass
    class Item(_Item): pass
    class AdditionalComment(_AdditionalComment): pass

    def __init__(
        self,
        number: 'CancelRequest.Number',
        order_number: OrderNumber,
        items: 'Tuple[CancelRequest.Item]',
        refund_method: RefundMethodAbstract,
        additional_comment: Optional[AdditionalComment] = None
    ) -> None:
        if not isinstance(number, CancelRequest.Number):
            raise ArgumentTypeException(self.__init__, 'number', number)

        if not isinstance(order_number, OrderNumber):
            raise ArgumentTypeException(self.__init__, 'order_number', order_number)

        if not isinstance(items, tuple) or sum([not isinstance(item, CancelRequest.Item) for item in items]) > 0:
            raise ArgumentTypeException(self.__init__, 'items', items)
        elif len(items) == 0:
            raise ArgumentCannotBeEmptyException(self.__init__, 'items')

        if not isinstance(refund_method, RefundMethodAbstract):
            raise ArgumentTypeException(self.__init__, 'refund_method', refund_method)

        if additional_comment is not None and not isinstance(additional_comment, CancelRequest.AdditionalComment):
            raise ArgumentTypeException(self.__init__, 'additional_comment', additional_comment)

        self.__number = number
        self.__order_number = order_number
        self.__items = items
        self.__refund_method = refund_method
        self.__additional_comment = additional_comment
        self.__requested_at = datetime.datetime.now()

    @property
    def number(self) -> 'CancelRequest.Number':
        return self.__number

    @property
    def order_number(self) -> OrderNumber:
        return self.__order_number

    @property
    def items(self) -> 'Tuple[CancelRequest.Item]':
        return self.__items

    def get_item_qty(self, simple_sku: SimpleSku) -> Qty:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.get_item_qty, 'simple_sku', simple_sku)

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                return item.qty
        else:
            raise ApplicationLogicException('Cancel Request #{} does not have Product "{}"!'.format(
                self.__number.value,
                simple_sku.value
            ))

    @property
    def refund_method(self) -> RefundMethodAbstract:
        return self.__refund_method

    @property
    def additional_comment(self) -> Optional['CancelRequest.AdditionalComment']:
        return self.__additional_comment

    @property
    def requested_at(self) -> datetime.datetime:
        return self.__requested_at

    @property
    def total_status(self) -> 'CancelRequest.Item.Status':
        items_statuses_map = {}
        for item in self.items:
            items_statuses_map[item.status.value] = items_statuses_map.get(item.status.value) or 0
            items_statuses_map[item.status.value] += 1

        """"""
        # Priority is important!
        # We skip one by one final statuses first.
        # If still no result - get the earliest status.
        see_setter = CancelRequest.Item.status
        """"""
        ignore_statuses = (
            CancelRequest.Item.Status.DECLINED,
            CancelRequest.Item.Status.APPROVED,
            CancelRequest.Item.Status.PENDING_APPROVAL,
        )
        for ignore_status in ignore_statuses:
            if len(items_statuses_map.keys()) == 1:
                all_items_status = CancelRequest.Item.Status(tuple(items_statuses_map.keys())[0])
                return all_items_status

            if ignore_status in items_statuses_map.keys():
                del items_statuses_map[ignore_status]

        raise Exception('Total status cannot be calculated for Cancel Request #{}! Set of Items Statuses: {}'.format(
            self.number.value,
            [item.status.value for item in self.items]
        ))

    def approve_item(self, simple_sku: SimpleSku) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.approve_item, 'simple_sku', simple_sku)

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.approve()
                break
        else:
            error_message = 'Cancel Request #{} cannot mark item "{}" {}, because item does not exist!'
            raise ApplicationLogicException(error_message.format(self.__number.value, simple_sku.value, 'Approved'))

    def decline_item(self, simple_sku: SimpleSku) -> None:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.decline_item, 'simple_sku', simple_sku)

        for item in self.__items:
            if item.simple_sku.value == simple_sku.value:
                item.decline()
                break
        else:
            error_message = 'Cancel Request #{} cannot mark item "{}" {}, because item does not exist!'
            raise ApplicationLogicException(error_message.format(self.__number.value, simple_sku.value, 'Declined'))


# ----------------------------------------------------------------------------------------------------------------------


class CancelRequestStorageInterface(object):
    def save(self, cancel_request: CancelRequest) -> None:
        raise NotImplementedError()

    def get_by_number(self, request_number: CancelRequest.Number) -> Optional[CancelRequest]:
        raise NotImplementedError()

    def get_all_by_order_number(self, order_number: OrderNumber) -> Tuple[CancelRequest]:
        raise NotImplementedError()

