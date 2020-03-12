from typing import Optional, Tuple
from chalicelib.extensions import *
from .values import Id, Name, Email, DeliveryAddress
from .customer_tier import CustomerTier


class _CustomerDeliveryAddress(object):
    ADDRESS_TYPE_RESIDENTIAL: str = 'residential'
    ADDRESS_TYPE_BUSINESS: str = 'business'

    def __init__(
        self,
        address_type: str,
        delivery_address: DeliveryAddress,
        address_nickname: Optional[str],
        is_billing: bool,
        is_shipping: bool
    ):
        if not isinstance(delivery_address, DeliveryAddress):
            raise ArgumentTypeException(self.__init__, 'delivery_address', delivery_address)

        __address_types = (self.ADDRESS_TYPE_BUSINESS, self.ADDRESS_TYPE_RESIDENTIAL)
        if address_type not in __address_types:
            raise ArgumentUnexpectedValueException(address_type, __address_types)

        if address_nickname is not None and not isinstance(address_nickname, str):
            raise ArgumentTypeException(self.__init__, 'address_nickname', address_nickname)

        if not isinstance(is_billing, bool):
            raise ArgumentTypeException(self.__init__, 'is_billing', is_billing)

        if not isinstance(is_shipping, bool):
            raise ArgumentTypeException(self.__init__, 'is_shipping', is_shipping)

        self.__address_type = address_type
        self.__delivery_address = delivery_address
        self.__address_nickname = str(address_nickname).strip() or None if address_nickname is not None else None
        self.__is_billing = is_billing
        self.__is_shipping = is_shipping

    @property
    def is_billing(self) -> bool:
        return self.__is_billing

    @property
    def is_shipping(self) -> bool:
        return self.__is_shipping

    @property
    def address_type(self) -> str:
        return self.__address_type

    @property
    def address_nickname(self) -> str:
        return self.__address_nickname

    @property
    def address_hash(self) -> str:
        return self.__delivery_address.address_hash

    @property
    def recipient_name(self) -> str:
        return self.__delivery_address.recipient_name

    @property
    def phone_number(self) -> str:
        return self.__delivery_address.phone_number

    @property
    def street_address(self) -> str:
        return self.__delivery_address.street_address

    @property
    def suburb(self) -> str:
        return self.__delivery_address.suburb

    @property
    def city(self) -> str:
        return self.__delivery_address.city

    @property
    def province(self) -> str:
        return self.__delivery_address.province

    @property
    def complex_building(self) -> str:
        return self.__delivery_address.complex_building

    @property
    def postal_code(self) -> str:
        return self.__delivery_address.postal_code

    @property
    def business_name(self) -> Optional[str]:
        return self.__delivery_address.business_name

    @property
    def special_instructions(self) -> str:
        return self.__delivery_address.special_instructions


# ----------------------------------------------------------------------------------------------------------------------


class _CustomerName(object):
    def __init__(self, first_name: Name, last_name: Name):
        if not isinstance(first_name, Name):
            raise ArgumentTypeException(self.__init__, 'first_name', first_name)
        if not isinstance(last_name, Name):
            raise ArgumentTypeException(self.__init__, 'last_name', last_name)

        self.__first_name = first_name
        self.__last_name = last_name

    @property
    def first_name(self) -> Name:
        return self.__first_name

    @property
    def last_name(self) -> Name:
        return self.__last_name

    @property
    def full_name(self) -> Name:
        """First_name Last_name"""
        return Name(self.__first_name.value + ' ' + self.__last_name.value)


# ----------------------------------------------------------------------------------------------------------------------


class _CustomerGender(object):
    MALE = 'male'
    FEMALE = 'female'

    __LIST = {
        MALE: 'Male',
        FEMALE: 'Female',
    }

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'value', value)
        elif value not in self.__class__.__LIST.keys():
            raise ArgumentUnexpectedValueException(value, tuple(self.__class__.__LIST.keys()))

        self.__value = value

    @property
    def descriptor(self) -> str:
        return self.__value

    @property
    def label(self) -> str:
        return self.__class__.__LIST[self.__value]


# ----------------------------------------------------------------------------------------------------------------------


class CustomerInterface(object):
    class Name(_CustomerName): pass
    class DeliveryAddress(_CustomerDeliveryAddress): pass
    class Gender(_CustomerGender): pass

    @property
    def customer_id(self) -> Id:
        raise NotImplementedError()

    @property
    def email(self) -> Email:
        raise NotImplementedError()

    @property
    def gender(self) -> Optional[Gender]:
        raise NotImplementedError()

    @property
    def name(self) -> Optional['CustomerInterface.Name']:
        raise NotImplementedError()

    @property
    def tier(self) -> CustomerTier:
        raise NotImplementedError()

    @tier.setter
    def tier(self, tier: CustomerTier) -> None:
        raise NotImplementedError()

    @property
    def delivery_addresses(self) -> Tuple['CustomerInterface.DeliveryAddress']:
        raise NotImplementedError()

    def add_delivery_address(self, delivery_address: 'CustomerInterface.DeliveryAddress') -> None:
        raise NotImplementedError()

    def remove_delivery_address(self, address_hash) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class CustomerStorageInterface(object):
    def save(self, customer: CustomerInterface) -> None:
        raise NotImplementedError()

    def get_by_id(self, customer_id: Id) -> Optional[CustomerInterface]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------

