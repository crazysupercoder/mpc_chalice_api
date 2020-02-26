from typing import Optional
from datetime import date
from chalicelib.extensions import *
from .values import Name, Description, SimpleSku, Qty


# ----------------------------------------------------------------------------------------------------------------------


class _Occasion(object):
    def __init__(self, name: Name, description: Description):
        if not isinstance(name, Name):
            raise ArgumentTypeException(self.__init__, 'name', name)

        if not isinstance(description, Description):
            raise ArgumentTypeException(self.__init__, 'description', description)

        self.__name = name
        self.__description = description

    @property
    def name(self) -> Name:
        return self.__name

    @property
    def description(self) -> Description:
        return self.__description


# ----------------------------------------------------------------------------------------------------------------------


class Dtd(object):
    class Occasion(_Occasion): pass

    def __init__(
        self,
        occasion: Optional['Dtd.Occasion'],
        date_from: date,
        date_to: date,
        working_days_from: int,
        working_days_to: int
    ) -> None:
        # occasion
        if occasion is not None and not isinstance(occasion, Dtd.Occasion):
            raise ArgumentTypeException(self.__init__, 'occasion', occasion)

        # dates
        if not isinstance(date_from, date):
            raise ArgumentTypeException(self.__init__, 'date_from', date_from)
        elif not isinstance(date_to, date):
            raise ArgumentTypeException(self.__init__, 'date_to', date_to)
        elif date_to < date_from:
            raise ArgumentValueException('{0} expects {1} < {2}, but {1} > {2}'.format(
                self.__init__.__qualname__,
                date_from,
                date_to
            ))

        # days
        if not isinstance(working_days_from, int):
            raise ArgumentTypeException(self.__init__, 'working_days_from', working_days_from)
        elif working_days_from < 1:
            raise ArgumentValueException('{0} "{1}" parameter cannot be < 1'.format(
                self.__init__.__qualname__,
                'working_days_from'
            ))
        elif not isinstance(working_days_to, int):
            raise ArgumentTypeException(self.__init__, 'working_days_to', working_days_to)
        elif working_days_to < 1:
            raise ArgumentValueException('{0} "{1}" parameter cannot be < 1'.format(
                self.__init__.__qualname__,
                'working_days_to'
            ))
        elif working_days_to < working_days_from:
            raise ArgumentValueException('{0} "{1}" parameter cannot be < "{2}" parameter'.format(
                self.__init__.__qualname__,
                'working_days_to',
                'working_days_from',
            ))

        self.__occasion = occasion
        self.__date_from = date_from
        self.__date_to = date_to
        self.__working_days_from = working_days_from
        self.__working_days_to = working_days_to

    @property
    def occasion(self) -> Optional['Dtd.Occasion']:
        return self.__occasion

    @property
    def date_from(self) -> date:
        # datetime objects are already immutable
        return self.__date_from

    @property
    def date_to(self) -> date:
        # datetime objects are already immutable
        return self.__date_to

    @property
    def working_days_from(self) -> int:
        return self.__working_days_from

    @property
    def working_days_to(self) -> int:
        return self.__working_days_to


# ----------------------------------------------------------------------------------------------------------------------


class DtdCalculatorInterface(object):
    def get_default(self) -> Dtd:
        raise NotImplementedError()

    def calculate(self, simple_sku: SimpleSku, qty: Qty) -> Dtd:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

