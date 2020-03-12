from chalicelib.extensions import *
from chalicelib.libs.purchase.core import RefundMethodAbstract


class StoreCreditRefundMethod(RefundMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'store_credit'

    @classmethod
    def _get_label(cls):
        return 'Store Credit'

    @property
    def extra_data(self) -> dict:
        return {}


class EftRefundMethod(RefundMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'credit_card_eft'

    @classmethod
    def _get_label(cls):
        return 'Credit Card / EFT'

    def __init__(self, account_holder_name: str, account_number: str, branch_code: str) -> None:
        arguments_map = {
            'account_holder_name': account_holder_name,
            'account_number': account_number,
            'branch_code': branch_code,
        }
        for argument_name in tuple(arguments_map.keys()):
            argument_value = arguments_map[argument_name]
            if not isinstance(argument_value, str):
                raise ArgumentTypeException(self.__init__, argument_name, argument_value)
            elif not argument_value.strip():
                raise ArgumentCannotBeEmptyException(self.__init__, argument_name)

        self.__account_holder_name = account_holder_name
        self.__account_number = account_number
        self.__branch_code = branch_code

    @property
    def extra_data(self) -> dict:
        return {
            'account_holder_name': self.__account_holder_name,
            'account_number': self.__account_number,
            'branch_code': self.__branch_code,
        }


class CreditCardRefundMethod(RefundMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'credit_card'

    @classmethod
    def _get_label(cls):
        return 'Credit Card'

    @property
    def extra_data(self) -> dict:
        return {}


class MobicredRefundMethod(RefundMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'mobicred'

    @classmethod
    def _get_label(cls):
        return 'Mobicred'

    @property
    def extra_data(self) -> dict:
        return {}

