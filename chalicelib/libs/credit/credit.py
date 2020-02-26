from typing import Optional
from chalicelib.extensions import *


# @todo : refactoring! Credits must be a part of purchase customer, not separated thing
# @todo : perhaps, value object {paid, earned, fbucks} like cost - need for refunds also
# @todo : perhaps, there should be requests history to subtract total amount of active requests from available


# ----------------------------------------------------------------------------------------------------------------------


class CreditCashOutRequest(object):
    def __init__(
        self,
        email: str,
        amount: float,
        account_holder_name: str,
        account_number: int,
        branch_code: str
    ) -> None:
        if not isinstance(email, str):
            raise ArgumentTypeException(self.__init__, 'email', email)

        if not isinstance(amount, float):
            raise ArgumentTypeException(self.__init__, 'amount', amount)

        if not isinstance(account_holder_name, str):
            raise ArgumentTypeException(self.__init__, 'account_holder_name', account_holder_name)

        if not isinstance(account_number, int):
            raise ArgumentTypeException(self.__init__, 'account_number', account_number)

        if not isinstance(branch_code, str):
            raise ArgumentTypeException(self.__init__, 'branch_code', branch_code)

        self.__email = email
        self.__amount = amount
        self.__account_holder_name = account_holder_name
        self.__account_number = account_number
        self.__branch_code = branch_code

    @property
    def email(self) -> str:
        return self.__email

    @email.setter
    def email(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'email', value)
        self.__email = value

    @property
    def amount(self) -> float:
        return self.__amount
        
    @amount.setter
    def amount(self, value: float):
        if not isinstance(value, float):
            raise ArgumentTypeException(self.__init__, 'amount', value)
        self.__amount = value

    @property
    def account_holder_name(self) -> str:
        return self.__account_holder_name
        
    @account_holder_name.setter
    def account_holder_name(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'account_holder_name', value)
        self.__account_holder_name = value

    @property
    def account_number(self) -> int:
        return self.__account_number
        
    @account_number.setter
    def account_number(self, value: int):
        if not isinstance(value, int):
            raise ArgumentTypeException(self.__init__, 'account_number', value)
        self.__account_number = value

    @property
    def branch_code(self) -> str:
        return self.__branch_code
        
    @branch_code.setter
    def branch_code(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'branch_code', value)
        self.__branch_code = value


# ----------------------------------------------------------------------------------------------------------------------


class Credit(object):
    def __init__(
        self,
        email: str,
        earned: float,
        paid: float
    ) -> None:
        if not isinstance(email, str):
            raise ArgumentTypeException(self.__init__, 'email', email)

        if not isinstance(earned, float):
            raise ArgumentTypeException(self.__init__, 'earned', earned)

        if not isinstance(paid, float):
            raise ArgumentTypeException(self.__init__, 'paid', paid)

        self.__email = email
        self.__earned = earned
        self.__paid = paid

    @property
    def email(self) -> str:
        return self.__email

    @email.setter
    def email(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'email', value)
        self.__email = value

    @property
    def earned(self) -> float:
        return self.__earned
        
    @earned.setter
    def earned(self, value: float):
        if not isinstance(value, float):
            raise ArgumentTypeException(self.__init__, 'earned', value)
        self.__earned = value

    @property
    def paid(self) -> float:
        return self.__paid
        
    @paid.setter
    def paid(self, value: float):
        if not isinstance(value, float):
            raise ArgumentTypeException(self.__init__, 'paid', value)
        self.__paid = value


# ----------------------------------------------------------------------------------------------------------------------


class CreditStorageInterface(object):
    def save(self, credit: Credit) -> None:
        raise NotImplementedError()

    def load(self, email: str) -> Optional[Credit]:
        raise NotImplementedError()

