import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *
from .values import Id, OrderNumber, SimpleSku, Qty, Cost, Description
from .payments import RefundMethodAbstract


class _Number(Id):
    def __init__(self, value: str):
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        if str(int(str(value))) != str(value):
            raise ArgumentValueException(
                # Year, Day of year, MPC store id, microseconds = 13 digits
                self.__init__.__qualname__ + ' expects [0-9]{13}, but ' + str(value) + ' is given!'
            )

        super().__init__(value)


class _Status(object):
    PENDING_APPROVAL = 'pending_approval'
    APPROVED = 'approved'
    REJECTED = 'rejected'
    PACKAGE_SENT = 'package_sent'
    CLOSED = 'closed'
    CANCELLED = 'cancelled'

    __LIST = {
        PENDING_APPROVAL: 'Pending Approval',
        APPROVED: 'Approved',
        REJECTED: 'Rejected',
        PACKAGE_SENT: 'Package Sent',
        CLOSED: 'Closed',
        CANCELLED: 'Cancelled',
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


class _StatusChangesHistory(object):
    class Change(object):
        def __init__(self, status: 'ReturnRequest.Item.Status'):
            if not isinstance(status, ReturnRequest.Item.Status):
                raise ArgumentTypeException(self.__init__, 'status', status)

            self.__status = status
            self.__datetime = datetime.datetime.now()

        @property
        def status(self) -> 'ReturnRequest.Item.Status':
            return self.__status

        @property
        def datetime(self) -> datetime.datetime:
            # datetime objects are already immutable
            return self.__datetime

    def __init__(self, changes: Tuple['ReturnRequest.Item.StatusChangesHistory.Change']) -> None:
        self.__changes = []
        for change in changes:
            if not isinstance(change, ReturnRequest.Item.StatusChangesHistory.Change):
                raise ArgumentTypeException(self.__init__, 'changes', changes)

            self.__changes.append(change)

    def add(self, change: 'ReturnRequest.Item.StatusChangesHistory.Change') -> None:
        if not isinstance(change, ReturnRequest.Item.StatusChangesHistory.Change):
            raise ArgumentTypeException(self.add, 'change', change)

        self.__changes.append(change)

    def get_last(self) -> Optional['ReturnRequest.Item.StatusChangesHistory.Change']:
        return self.__changes[-1] if self.__changes else None

    def get_last_concrete(self, status_value: str) -> Optional['ReturnRequest.Item.StatusChangesHistory.Change']:
        for i in range(len(self.__changes) - 1, -1, -1):
            if self.__changes[i].status.value == status_value:
                return self.__changes[i]
        else:
            return None

    def get_all(self) -> Tuple['ReturnRequest.Item.StatusChangesHistory.Change']:
        return tuple(self.__changes)


class _ReturnReason(object):
    TOO_BIG = 'too_big'
    TOO_SMALL = 'too_small'
    SELECTED_WRONG_SIZE = 'selected_wrong_size'
    DONT_LIKE_IT = 'do_not_like_it'
    NOT_HAPPY_WITH_QTY = 'not_happy_with_qty'
    RECEIVED_WRONG_SIZE = 'received_wrong_size_item'
    RECEIVED_DAMAGED = 'received_item_damaged'

    __LIST = {
        TOO_BIG: 'Too Big',
        TOO_SMALL: 'Too Small',
        SELECTED_WRONG_SIZE: 'I selected wrong size',
        DONT_LIKE_IT: 'I don\'t like it',
        NOT_HAPPY_WITH_QTY: 'I\'m not happy with the quantity',
        RECEIVED_WRONG_SIZE: 'I received the wrong size/item',
        RECEIVED_DAMAGED: 'I received the item damaged',
    }

    def __init__(self, descriptor):
        known_values = tuple(self.__class__.__LIST.keys())
        if descriptor not in known_values:
            raise ArgumentUnexpectedValueException(descriptor, known_values)

        self.__descriptor = descriptor

    @property
    def descriptor(self) -> str:
        return self.__descriptor

    @property
    def label(self) -> str:
        return self.__class__.__LIST[self.descriptor]


class _DeliveryMethod(object):
    @classmethod
    def _get_descriptor(cls):
        raise NotImplementedError()

    @classmethod
    def _get_label(cls):
        raise NotImplementedError()

    @property
    def descriptor(self) -> str:
        return self.__class__._get_descriptor()

    @property
    def label(self) -> str:
        return self.__class__._get_label()


class _AdditionalComment(Description):
    pass


class _AttachedFile(object):
    def __init__(self, url: str) -> None:
        if not isinstance(url, str):
            raise ArgumentTypeException(self.__init__, 'url', url)
        elif not url.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'url')

        self.__url = url

    @property
    def url(self) -> str:
        return self.__url


class _Item(object):
    class Status(_Status): pass
    class StatusChangesHistory(_StatusChangesHistory): pass
    class Reason(_ReturnReason): pass
    class AdditionalComment(_AdditionalComment): pass
    class AttachedFile(_AttachedFile): pass

    def __init__(
        self,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        qty: Qty,
        cost: Cost,
        reason: Reason,
        attached_files: Tuple[AttachedFile] = tuple(),
        additional_comment: Optional[AdditionalComment] = None
    ):
        if not isinstance(order_number, OrderNumber):
            raise ArgumentTypeException(self.__init__, 'order_number', order_number)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.__init__, 'simple_sku', simple_sku)

        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.__init__, 'qty', qty)
        elif qty.value == 0:
            raise ArgumentValueException('{} cannot work with qty = 0'.format(self.__init__.__qualname__))

        if not isinstance(cost, Cost):
            raise ArgumentTypeException(self.__init__, 'cost', cost)

        if not isinstance(reason, _Item.Reason):
            raise ArgumentTypeException(self.__init__, 'reason', reason)

        if not isinstance(attached_files, tuple):
            raise ArgumentTypeException(self.__init__, 'attached_files', attached_files)
        elif sum([not isinstance(attached_file, _Item.AttachedFile) for attached_file in attached_files]) > 0:
            raise ArgumentTypeException(self.__init__, 'attached_files', attached_files)

        if additional_comment is not None and not isinstance(additional_comment, _Item.AdditionalComment):
            raise ArgumentTypeException(self.__init__, 'additional_comment', additional_comment)

        self.__order_number = order_number
        self.__simple_sku = simple_sku
        self.__qty = qty
        self.__cost = cost
        # ---
        self.__reason = reason
        self.__attached_files = attached_files
        self.__additional_comment = additional_comment
        # ---
        self.__status_history = _Item.StatusChangesHistory((
            _Item.StatusChangesHistory.Change(_Item.Status(_Item.Status.PENDING_APPROVAL)),
        ))

    @property
    def order_number(self) -> OrderNumber:
        return self.__order_number

    @property
    def simple_sku(self) -> SimpleSku:
        return self.__simple_sku

    @property
    def qty(self) -> Qty:
        return self.__qty

    @property
    def cost(self) -> Cost:
        return self.__cost

    @property
    def reason(self) -> '_Item.Reason':
        return self.__reason

    @property
    def attached_files(self) -> 'Tuple[_Item.AttachedFile]':
        return self.__attached_files

    @property
    def additional_comment(self) -> 'Optional[_Item.AdditionalComment]':
        return self.__additional_comment

    @property
    def requested_at(self) -> datetime:
        return self.__status_history.get_last_concrete(_Item.Status.PENDING_APPROVAL).datetime

    @property
    def status(self) -> '_Item.Status':
        return self.__status_history.get_last().status

    @status.setter
    def status(self, new_status: '_Item.Status') -> None:
        self.__set_status(new_status)

    def __set_status(self, new_status: '_Item.Status') -> None:
        if not isinstance(new_status, _Item.Status):
            raise ArgumentTypeException(self.__set_status, 'new_status', new_status)

        changes_map = {
            _Item.Status.PENDING_APPROVAL: (_Item.Status.APPROVED, _Item.Status.CANCELLED, _Item.Status.REJECTED),
            # @todo : not sure about "package sent" status. Perhaps, it is not needed or can be skipped.
            _Item.Status.APPROVED: (_Item.Status.PACKAGE_SENT, _Item.Status.REJECTED, _Item.Status.CLOSED,),
            _Item.Status.PACKAGE_SENT: (_Item.Status.CLOSED,),
            _Item.Status.CLOSED: (),
            _Item.Status.CANCELLED: (),
            _Item.Status.REJECTED: (),
        }

        allowed_new_values = changes_map.get(self.status.value, None)
        if allowed_new_values is None:
            raise Exception('{} does not know, how to work with {} status!'.format(
                self.__set_status,
                self.status.value
            ))

        if new_status.value not in allowed_new_values:
            raise ApplicationLogicException('Return Request item cannot be turned to "{}" state from "{}"!'.format(
                new_status.label,
                self.status.label
            ))

        change = ReturnRequest.Item.StatusChangesHistory.Change(new_status)
        self.__status_history.add(change)

    @property
    def is_processable(self) -> bool:
        return self.status.value not in [
            self.__class__.Status.CLOSED,
            self.__class__.Status.CANCELLED,
            self.__class__.Status.REJECTED,
        ]

# ----------------------------------------------------------------------------------------------------------------------


class ReturnRequest(object):
    class Number(_Number): pass
    class Item(_Item): pass
    class DeliveryMethod(_DeliveryMethod): pass

    def __init__(
        self,
        customer_id: Id,
        request_number: 'ReturnRequest.Number',
        items: 'Tuple[ReturnRequest.Item]',
        delivery_method: DeliveryMethod,
        refund_method: RefundMethodAbstract
    ) -> None:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.__init__, 'customer_id', customer_id)

        if not isinstance(request_number, ReturnRequest.Number):
            raise ArgumentTypeException(self.__init__, 'request_number', request_number)

        if not isinstance(items, tuple) or sum([not isinstance(item, ReturnRequest.Item) for item in items]) > 0:
            raise ArgumentTypeException(self.__init__, 'items', items)
        elif len(items) == 0:
            raise ArgumentCannotBeEmptyException(self.__init__, 'items')

        if not isinstance(delivery_method, ReturnRequest.DeliveryMethod):
            raise ArgumentTypeException(self.__init__, 'delivery_method', delivery_method)

        if not isinstance(refund_method, RefundMethodAbstract):
            raise ArgumentTypeException(self.__init__, 'refund_method', refund_method)

        self.__customer_id = customer_id
        self.__number = request_number
        self.__items = items
        self.__delivery_method = delivery_method
        self.__refund_method = refund_method

    @property
    def customer_id(self) -> Id:
        return self.__customer_id

    @property
    def number(self) -> 'ReturnRequest.Number':
        return self.__number

    @property
    def items(self) -> 'Tuple[ReturnRequest.Item]':
        return self.__items

    @property
    def delivery_method(self) -> 'ReturnRequest.DeliveryMethod':
        return self.__delivery_method

    @property
    def refund_method(self) -> RefundMethodAbstract:
        return self.__refund_method

    @property
    def requested_at(self) -> datetime.datetime:
        return self.__items[0].requested_at

    @property
    def total_status(self) -> 'ReturnRequest.Item.Status':
        items_statuses_map = {}
        for item in self.items:
            items_statuses_map[item.status.value] = items_statuses_map.get(item.status.value) or 0
            items_statuses_map[item.status.value] += 1

        """"""
        # Priority is important!
        # We skip one by one final statuses first.
        # If still no result - get the earliest status.
        see_setter = ReturnRequest.Item.status
        """"""
        ignore_statuses = (
            ReturnRequest.Item.Status.CANCELLED,
            ReturnRequest.Item.Status.REJECTED,
            ReturnRequest.Item.Status.CLOSED,
            ReturnRequest.Item.Status.PACKAGE_SENT,
            ReturnRequest.Item.Status.APPROVED,
            ReturnRequest.Item.Status.PENDING_APPROVAL,
        )
        for ignore_status in ignore_statuses:
            if len(items_statuses_map.keys()) == 1:
                all_items_status = ReturnRequest.Item.Status(tuple(items_statuses_map.keys())[0])
                return all_items_status

            if ignore_status in items_statuses_map.keys():
                del items_statuses_map[ignore_status]

        raise Exception('Total status cannot be calculated for Return Request #{}! Set of Items Statuses: {}'.format(
            self.number.value,
            [item.status.value for item in self.items]
        ))

    def __change_item_status(
        self,
        order_number: OrderNumber,
        simple_sku: SimpleSku,
        status: 'ReturnRequest.Item.Status'
    ) -> None:
        if not isinstance(order_number, OrderNumber):
            raise ArgumentTypeException(self.__change_item_status, 'order_number', order_number)

        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.__change_item_status, 'simple_sku', simple_sku)

        if not isinstance(status, ReturnRequest.Item.Status):
            raise ArgumentTypeException(self.__change_item_status, 'status', status)

        for item in self.__items:
            if item.order_number == order_number and item.simple_sku == simple_sku:
                item.status = status
                break
        else:
            error_message = 'Return Request #{} cannot mark item #{} / {} as "{}", because item does not exist!'
            raise ApplicationLogicException(error_message.format(
                self.__number.value,
                order_number.value,
                simple_sku.value,
                status.value
            ))

    def get_item_qty(self, order_number: OrderNumber, simple_sku: SimpleSku) -> Qty:
        for item in self.__items:
            if item.order_number == order_number and item.simple_sku == simple_sku:
                return item.qty
        else:
            raise ApplicationLogicException('Return Request #{} has no item #{} / {}!'.format(
                self.__number.value,
                order_number.value,
                simple_sku.value,
            ))

    def make_item_approved(self, order_number: OrderNumber, simple_sku: SimpleSku) -> None:
        status = ReturnRequest.Item.Status(ReturnRequest.Item.Status.APPROVED)
        self.__change_item_status(order_number, simple_sku, status)

    def make_package_sent(self) -> None:
        for item in self.items:
            if item.is_processable:
                item.status = ReturnRequest.Item.Status(ReturnRequest.Item.Status.PACKAGE_SENT)

    def make_item_closed(self, order_number: OrderNumber, simple_sku: SimpleSku) -> None:
        status = ReturnRequest.Item.Status(ReturnRequest.Item.Status.CLOSED)
        self.__change_item_status(order_number, simple_sku, status)

    def make_item_rejected(self, order_number: OrderNumber, simple_sku: SimpleSku) -> None:
        status = ReturnRequest.Item.Status(ReturnRequest.Item.Status.REJECTED)
        self.__change_item_status(order_number, simple_sku, status)

    def make_item_declined(self, order_number: OrderNumber, simple_sku: SimpleSku) -> None:
        status = ReturnRequest.Item.Status(ReturnRequest.Item.Status.CANCELLED)
        self.__change_item_status(order_number, simple_sku, status)


# ----------------------------------------------------------------------------------------------------------------------


class ReturnRequestStorageInterface(object):
    def save(self, return_request: ReturnRequest) -> None:
        raise NotImplementedError()

    def load(self, request_number: ReturnRequest.Number) -> Optional[ReturnRequest]:
        raise NotImplementedError()

    def get_all_for_customer(self, customer_id: Id) -> Tuple[ReturnRequest]:
        raise NotImplementedError()

