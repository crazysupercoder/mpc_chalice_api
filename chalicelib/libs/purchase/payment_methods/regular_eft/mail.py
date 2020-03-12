from typing import Tuple
from chalicelib.extensions import *
from chalicelib.libs.core.mailer import MailMessageInterface
from chalicelib.libs.purchase.core import Order
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation


class RegularEftBankDetailsMailMessage(MailMessageInterface):
    # @todo : update implementation!

    def __init__(self, order: Order):
        if not isinstance(order, Order):
            raise ArgumentTypeException(self.__init__, 'order', order)

        self.__order = order
        self.__customer = CustomerStorageImplementation().get_by_id(order.customer_id)

    @property
    def to_email(self) -> str:
        return self.__customer.email.value

    @property
    def subject(self) -> str:
        return 'EFT Payment info for Order #{}'.format(self.__order.number)

    @property
    def content(self) -> str:
        content = "Hi, {}!\r\n\r\n".format(self.__customer.name or 'Customer')
        content = content + self.subject
        return content

    @property
    def paths_to_attachments(self) -> Tuple[str]:
        order_number = self.__order.number.value
        path_to_file = '/tmp/eft_payment_info_{}.txt'.format(order_number)
        with open(path_to_file, "w+") as file:
            file.write('EFT Payment info for Order #{}'.format(order_number))

        return tuple([path_to_file])

