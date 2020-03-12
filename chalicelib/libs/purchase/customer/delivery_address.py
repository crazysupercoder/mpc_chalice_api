from typing import List, Tuple
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Id, DeliveryAddress, CustomerInterface, CustomerStorageInterface


# ----------------------------------------------------------------------------------------------------------------------


class CustomerDeliveryAddressForm(object):
    # create 2 classes, if there will be more difference
    ADDRESS_TYPE_RESIDENTIAL: str = 'residential'
    ADDRESS_TYPE_BUSINESS: str = 'business'

    def __init__(self, address_type: str):
        """
        :raises ArgumentTypeException:
        :raises ArgumentUnexpectedValueException:
        """
        __supported_address_types = (self.ADDRESS_TYPE_RESIDENTIAL, self.ADDRESS_TYPE_BUSINESS)
        if not isinstance(address_type, str):
            raise ArgumentTypeException(self.__init__, 'address_type', address_type)
        elif address_type not in __supported_address_types:
            raise ArgumentUnexpectedValueException(address_type, __supported_address_types)

        self.__address_type = address_type
        self.__validation_errors = []

        self.recipient_name = None
        self.phone_number = None
        self.street_address = None
        self.suburb = None
        self.city = None
        self.province = None
        self.complex_building = None
        self.postal_code = None
        self.business_name = None
        self.special_instructions = None
        self.address_nickname = None
        self.is_billing = False
        self.is_shipping = False

    @property
    def address_type(self) -> str:
        return self.__address_type

    def load(self, data: dict) -> None:
        def __str_or_none(s):
            return str(s).strip() if s is not None and str(s).strip() else None

        self.recipient_name = __str_or_none(data.get('recipient_name', ''))
        self.phone_number = __str_or_none(data.get('phone_number', ''))
        self.street_address = __str_or_none(data.get('street_address', ''))
        self.suburb = __str_or_none(data.get('suburb', ''))
        self.city = __str_or_none(data.get('city', ''))
        self.province = __str_or_none(data.get('province', ''))
        self.complex_building = __str_or_none(data.get('complex_building', ''))
        self.postal_code = __str_or_none(data.get('postal_code', ''))
        self.business_name = __str_or_none(data.get('business_name', ''))
        self.special_instructions = __str_or_none(data.get('special_instructions', ''))
        self.address_nickname = __str_or_none(data.get('address_nickname', ''))
        self.is_billing = bool(data.get('is_billing', False))
        self.is_shipping = bool(data.get('is_shipping', False))

    def validate(self) -> bool:
        self.__validation_errors = []

        required_attributes = {
            'recipient_name': self.recipient_name,
            'phone_number': self.phone_number,
            'street_address': self.street_address,
            'suburb': self.suburb,
            'city': self.city,
            'province': self.province,
        }

        for attribute_name in tuple(required_attributes.keys()):
            if not required_attributes.get(attribute_name):
                self.__validation_errors.append({
                    'attribute_name': attribute_name,
                    'error_message': 'Attribute is required!',
                })

        return len(self.__validation_errors) == 0

    @property
    def validation_errors(self) -> Tuple[dict]:
        """ [ {attribute_name: str, error_message: str}, ... ]"""
        validation_errors: List[dict] = self.__validation_errors
        return tuple(validation_errors)


# ----------------------------------------------------------------------------------------------------------------------


class CustomerDeliveryAddressAppService(object):

    def __init__(self, customer_storage: CustomerStorageInterface) -> None:
        if not isinstance(customer_storage, CustomerStorageInterface):
            raise ArgumentTypeException(self.__init__, 'customer_storage', customer_storage)

        self.__customer_storage = customer_storage

    def __get_customer(self, user_id: str) -> CustomerInterface:
        customer_id = Id(user_id)
        customer = self.__customer_storage.get_by_id(customer_id)
        if not customer:
            raise ApplicationLogicException('Customer does not exist!')

        return customer

    # ------------------------------------------------------------------------------------------------------------------

    def add_delivery_address(self, user_id: str, form: CustomerDeliveryAddressForm) -> None:
        if not isinstance(form, CustomerDeliveryAddressForm):
            raise ArgumentTypeException(self.add_delivery_address, 'form', form)
        elif not form.validate():
            raise ArgumentValueException('{0} must be valid for {1} action!'.format(
                form.__class__.__qualname__,
                self.add_delivery_address.__qualname__
            ))

        customer_delivery_address = CustomerInterface.DeliveryAddress(
            form.address_type,
            DeliveryAddress(
                form.recipient_name,
                form.phone_number,
                form.street_address,
                form.suburb,
                form.city,
                form.province,
                form.complex_building,
                form.postal_code,
                form.business_name,
                form.special_instructions
            ),
            form.address_nickname,
            form.is_billing,
            form.is_shipping
        )

        customer = self.__get_customer(user_id)
        customer.add_delivery_address(customer_delivery_address)
        self.__customer_storage.save(customer)

    # ------------------------------------------------------------------------------------------------------------------

    def edit_delivery_address(self, user_id: str, address_hash: str, form: CustomerDeliveryAddressForm) -> None:
        if not isinstance(form, CustomerDeliveryAddressForm):
            raise ArgumentTypeException(self.edit_delivery_address, 'form', form)
        elif not form.validate():
            raise ArgumentValueException('{0} must be valid for {1} action!'.format(
                CustomerDeliveryAddressForm.__class__.__name__,
                self.edit_delivery_address.__qualname__
            ))

        customer_delivery_address = CustomerInterface.DeliveryAddress(
            form.address_type,
            DeliveryAddress(
                form.recipient_name,
                form.phone_number,
                form.street_address,
                form.suburb,
                form.city,
                form.province,
                form.complex_building,
                form.postal_code,
                form.business_name,
                form.special_instructions
            ),
            form.address_nickname,
            form.is_billing,
            form.is_shipping
        )

        customer = self.__get_customer(user_id)
        customer.remove_delivery_address(address_hash)
        customer.add_delivery_address(customer_delivery_address)
        self.__customer_storage.save(customer)

    # ------------------------------------------------------------------------------------------------------------------

    def remove_delivery_address(self, user_id: str, address_hash: str) -> None:
        customer = self.__get_customer(user_id)
        customer.remove_delivery_address(address_hash)
        self.__customer_storage.save(customer)


# ----------------------------------------------------------------------------------------------------------------------

