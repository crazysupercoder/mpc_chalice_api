import re
import hashlib
from typing import Optional, Union
from chalicelib.extensions import *


# ----------------------------------------------------------------------------------------------------------------------


class _BaseSimpleValueObject(object):
    def __init__(self, value):
        self.__value = value

    def __str__(self) -> str:
        return str(self.value)

    def __eq__(self, other) -> bool:
        if type(self) is not type(other):
            return False
        else:
            return self.value == other.value

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    @property
    def value(self):
        return self.__value


class _BaseStringSimpleValueObject(_BaseSimpleValueObject):
    def __init__(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'value', value)

        super().__init__(value)


class _BaseNumberSimpleValueObject(_BaseSimpleValueObject):
    def __init__(self, value: Union[int, float]):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ArgumentTypeException(self.__init__, 'value', value)

        super().__init__(value)

    def __str__(self) -> str:
        return str(self.value)

    def __add__(self, other):
        if type(self) is not type(other):
            raise ArgumentTypeException(self.__add__, 'other', other)

        return self.__class__(self.value + other.value)

    def __sub__(self, other):
        if type(self) is not type(other):
            raise ArgumentTypeException(self.__sub__, 'other', other)

        return self.__class__(self.value - other.value)

    def __lt__(self, other) -> bool:
        if type(self) is not type(other):
            raise TypeError('Unable to compare {} and {} values!'.format(
                type(self),
                type(other)
            ))
        else:
            return self.value < other.value

    def __le__(self, other) -> bool:
        if type(self) is not type(other):
            raise TypeError('Unable to compare {} and {} values!'.format(
                type(self),
                type(other)
            ))
        else:
            return self.value <= other.value

    def __gt__(self, other) -> bool:
        if type(self) is not type(other):
            raise TypeError('Unable to compare {} and {} values!'.format(
                type(self),
                type(other)
            ))
        else:
            return self.value > other.value

    def __ge__(self, other) -> bool:
        if type(self) is not type(other):
            raise TypeError('Unable to compare {} and {} values!'.format(
                type(self),
                type(other)
            ))
        else:
            return self.value >= other.value


# ----------------------------------------------------------------------------------------------------------------------


class Id(_BaseStringSimpleValueObject):
    def __init__(self, value: str) -> None:
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> str: return super().value


# ----------------------------------------------------------------------------------------------------------------------


class Email(_BaseStringSimpleValueObject):
    def __init__(self, value: str) -> None:
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        pattern = r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'
        match = re.match(pattern, str(value).strip())
        if not match or not match.group():
            raise ArgumentValueException('{0} expects Email address string, "{1}" is given!'.format(
                self.__init__.__qualname__,
                value
            ))

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> str: return super().value


# ----------------------------------------------------------------------------------------------------------------------


class Name(_BaseStringSimpleValueObject):
    def __init__(self, value: str) -> None:
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> str: return super().value


# ----------------------------------------------------------------------------------------------------------------------


class Description(_BaseSimpleValueObject):
    def __init__(self, value: Optional[str]) -> None:
        if value is None:
            super().__init__(value)
            return

        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'value', value)

        if not str(value).strip():
            value = None

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> Optional[str]: return super().value


# ----------------------------------------------------------------------------------------------------------------------


class Percentage(_BaseNumberSimpleValueObject):
    def __init__(self, value: Union[float, int]) -> None:
        value = float(str(value))
        if value < 0 or value > 100:
            raise ArgumentValueException('{} value must be in range [0, 100]!'.format(self.__class__.__qualname__))

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> float: return super().value


# ----------------------------------------------------------------------------------------------------------------------


# @todo : move to product
class EventCode(_BaseStringSimpleValueObject):
    def __init__(self, value: str) -> None:
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        # @todo : add regex
        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> str: return super().value


# ----------------------------------------------------------------------------------------------------------------------


# @todo : move to product
class SimpleSku(_BaseStringSimpleValueObject):
    def __init__(self, value: str) -> None:
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        # @todo : add regex
        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> str: return super().value


# ----------------------------------------------------------------------------------------------------------------------


class Qty(_BaseNumberSimpleValueObject):
    def __init__(self, value: int) -> None:
        value = int(str(value))
        if value < 0:
            raise ArgumentValueException('{} value cannot be < 0!'.format(self.__class__.__qualname__))

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> int: return super().value
    def __add__(self, other: 'Qty') -> 'Qty': return super().__add__(other)
    def __sub__(self, other: 'Qty') -> 'Qty': return super().__sub__(other)


# ----------------------------------------------------------------------------------------------------------------------


class Cost(_BaseNumberSimpleValueObject):
    def __init__(self, value: Union[int, float, str]) -> None:
        value = float(str(value))
        if value < 0:
            raise ArgumentValueException('{0} value cannot be < 0!'.format(self.__class__.__qualname__))

        super().__init__(value)

    # just to annotate types
    @property
    def value(self) -> float: return super().value
    def __add__(self, other: 'Cost') -> 'Cost': return super().__add__(other)
    def __sub__(self, other: 'Cost') -> 'Cost': return super().__sub__(other)


# ----------------------------------------------------------------------------------------------------------------------


class DeliveryAddress(object):
    def __init__(
        self,
        recipient_name: str,
        phone_number: str,
        street_address: str,
        suburb: str,
        city: str,
        province: str,
        complex_building: str = None,
        postal_code: str = None,
        business_name: str = None,
        special_instructions: str = None
    ):
        # required params
        required_params = {
            'recipient_name': recipient_name,
            'phone_number': phone_number,
            'street_address': street_address,
            'suburb': suburb,
            'city': city,
            'province': province,
        }
        for key in tuple(required_params.keys()):
            value = str(required_params.get(key, '')).strip() or None
            if not value:
                raise ArgumentCannotBeEmptyException(self.__init__, key)

        # fix optional params
        def __str_or_none(s):
            return str(s).strip() if s is not None and str(s).strip() else None

        complex_building = __str_or_none(complex_building)
        postal_code = __str_or_none(postal_code)
        business_name = __str_or_none(business_name)
        special_instructions = __str_or_none(special_instructions)

        self.__recipient_name = recipient_name
        self.__phone_number = phone_number
        self.__street_address = street_address
        self.__suburb = suburb
        self.__city = city
        self.__province = province
        self.__complex_building = complex_building
        self.__postal_code = postal_code
        self.__business_name = business_name
        self.__special_instructions = special_instructions

        _hash = hashlib.md5()
        _hash.update(str(self.__dict__).encode('utf-8'))
        self.__hash = _hash.hexdigest()

    @property
    def recipient_name(self) -> str:
        return self.__recipient_name

    @property
    def phone_number(self) -> str:
        return self.__phone_number

    @property
    def street_address(self) -> str:
        return self.__street_address

    @property
    def suburb(self) -> str:
        return self.__suburb

    @property
    def city(self) -> str:
        return self.__city

    @property
    def province(self) -> str:
        return self.__province

    @property
    def complex_building(self) -> str:
        return self.__complex_building

    @property
    def postal_code(self) -> str:
        return self.__postal_code

    @property
    def business_name(self) -> Optional[str]:
        return self.__business_name

    @property
    def special_instructions(self) -> str:
        return self.__special_instructions

    @property
    def address_hash(self) -> str:
        return self.__hash


# ----------------------------------------------------------------------------------------------------------------------


class OrderNumber(Id):
    def __init__(self, value: str):
        if not str(value).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'value')

        if str(int(str(value))) != str(value):
            raise ArgumentValueException(
                self.__init__.__qualname__ + ' expects [0-9]{14}, but ' + str(value) + ' is given!'
            )

        super().__init__(value)


# ----------------------------------------------------------------------------------------------------------------------

