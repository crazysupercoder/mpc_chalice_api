from typing import Optional, Tuple
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import \
    Id, Email, Name, DeliveryAddress, \
    CustomerInterface, CustomerTier
from chalicelib.libs.models.mpc.Cms.Informations import Information


class _CustomerDeliveryAddressList(object):
    def __init__(self, items: Tuple[CustomerInterface.DeliveryAddress]):
        self.__items = []
        for item in items:
            if not isinstance(item, CustomerInterface.DeliveryAddress):
                raise ArgumentTypeException(self.__init__, 'items', items)

            self.__items.append(item)

    @property
    def items(self) -> Tuple[CustomerInterface.DeliveryAddress]:
        return tuple(self.__items)

    def add_address(self, new_address: CustomerInterface.DeliveryAddress) -> None:
        if not isinstance(new_address, CustomerInterface.DeliveryAddress):
            raise ArgumentTypeException(self.add_address, 'item', new_address)

        for old_address in self.__items:
            if old_address.address_hash == new_address.address_hash:
                raise ApplicationLogicException('Delivery Address already exists!')

        self.__items.append(new_address)

    def remove_address(self, address_hash: str) -> None:
        for address in self.__items:
            if address.address_hash == address_hash:
                index = self.__items.index(address)
                del self.__items[index]
                break
        else:
            raise ApplicationLogicException('Delivery Address does not exist!')


class CustomerImplementation(CustomerInterface):
    def __init__(self, customer_id: Id, information: Information, tier: CustomerTier):
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.__init__, 'customer_id', customer_id)
        if not isinstance(information, Information):
            raise ArgumentTypeException(self.__init__, 'information', information)

        self.__customer_id = customer_id
        self.__information = information
        self.__set_tier(tier)

        delivery_addresses = []
        for information_address in information.addresses:
            if information_address.business_type:
                address_type = CustomerInterface.DeliveryAddress.ADDRESS_TYPE_BUSINESS
            else:
                address_type = CustomerInterface.DeliveryAddress.ADDRESS_TYPE_RESIDENTIAL

            delivery_addresses.append(CustomerInterface.DeliveryAddress(
                address_type,
                DeliveryAddress(
                    information_address.recipient_name,
                    information_address.mobile_number,
                    information_address.street_address,
                    information_address.suburb,
                    information_address.city,
                    information_address.province,
                    information_address.complex_building or None,
                    information_address.postal_code or None,
                    information_address.business_name or None,
                    information_address.special_instructions or None
                ),
                information_address.address_nickname,
                information_address.is_default_billing,
                information_address.is_default_shipping
            ))
        self.__delivery_addresses_list = _CustomerDeliveryAddressList(tuple(delivery_addresses))

    def __set_tier(self, tier: CustomerTier) -> None:
        if not isinstance(tier, CustomerTier):
            raise ArgumentTypeException(self.__set_tier, 'tier', tier)

        self.__tier = tier

    @property
    def customer_id(self) -> Id:
        return self.__customer_id

    @property
    def email(self) -> Email:
        return Email(self.__information.email)

    @property
    def gender(self) -> Optional[CustomerInterface.Gender]:
        information_gender_value = self.__information.gender
        genders_map = {
            'male': CustomerInterface.Gender(CustomerInterface.Gender.MALE),
            'female': CustomerInterface.Gender(CustomerInterface.Gender.FEMALE),
        }

        if information_gender_value not in genders_map.keys():
            return None

        return genders_map[information_gender_value]

    @property
    def name(self) -> Optional[CustomerInterface.Name]:
        if not self.__information.first_name or not self.__information.last_name:
            return None

        return CustomerInterface.Name(
            Name(self.__information.first_name),
            Name(self.__information.last_name)
        )

    @property
    def tier(self) -> CustomerTier:
        return self.__tier

    @tier.setter
    def tier(self, tier: CustomerTier) -> None:
        self.__set_tier(tier)

    @property
    def delivery_addresses(self) -> Tuple[CustomerInterface.DeliveryAddress]:
        return self.__delivery_addresses_list.items

    def add_delivery_address(self, delivery_address: CustomerInterface.DeliveryAddress) -> None:
        self.__delivery_addresses_list.add_address(delivery_address)

    def remove_delivery_address(self, address_hash) -> None:
        self.__delivery_addresses_list.remove_address(address_hash)

