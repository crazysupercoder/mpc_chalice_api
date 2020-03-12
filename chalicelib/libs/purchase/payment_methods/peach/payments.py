from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Order


class _PeachPaymentMethod(Order.PaymentMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        raise NotImplementedError()

    @classmethod
    def _get_label(cls):
        raise NotImplementedError()

    def __init__(self, payment_id: str):
        if not isinstance(payment_id, str):
            raise ArgumentTypeException(self.__init__, 'payment_id', payment_id)

        self.__id = payment_id

    @property
    def extra_data(self) -> dict:
        return {
            'payment_id': self.__id
        }


class MobicredPaymentMethod(_PeachPaymentMethod):
    @classmethod
    def _get_descriptor(cls):
        return 'mobicred'

    @classmethod
    def _get_label(cls):
        return 'Mobicred'


class CreditCardOrderPaymentMethod(_PeachPaymentMethod):
    @classmethod
    def _get_descriptor(cls):
        return 'credit_card'

    @classmethod
    def _get_label(cls):
        return 'Credit Card'

