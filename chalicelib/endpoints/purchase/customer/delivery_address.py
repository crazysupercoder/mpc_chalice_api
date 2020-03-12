from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Id
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.purchase.customer.delivery_address import\
    CustomerDeliveryAddressForm, \
    CustomerDeliveryAddressAppService


def register_customer_delivery_addresses(blueprint: Blueprint) -> None:
    def __get_user_id() -> str:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user.id

    def __response_list(user_id: str) -> dict:
        customer_storage = CustomerStorageImplementation()
        customer_id = Id(user_id)
        customer = customer_storage.get_by_id(customer_id)

        delivery_addresses = [{
            'hash': delivery_address.address_hash,
            'address_type': delivery_address.address_type,
            'recipient_name': delivery_address.recipient_name,
            'phone_number': delivery_address.phone_number,
            'street_address': delivery_address.street_address,
            'suburb': delivery_address.suburb,
            'city': delivery_address.city,
            'province': delivery_address.province,
            'business_name': delivery_address.business_name,
            'complex_building': delivery_address.complex_building,
            'postal_code': delivery_address.postal_code,
            'special_instructions': delivery_address.special_instructions,
            'address_nickname': delivery_address.address_nickname,
            'is_billing': delivery_address.is_billing,
            'is_shipping': delivery_address.is_shipping,
        } for delivery_address in customer.delivery_addresses]

        return {
            'delivery_addresses': delivery_addresses,
        }

    def __create_delivery_address_form(address_type: str) -> CustomerDeliveryAddressForm:
        try:
            return CustomerDeliveryAddressForm(address_type)
        except ArgumentUnexpectedValueException as e:
            raise HttpIncorrectInputDataException(str(e))

    # ------------------------------------------------------------------------------------------------------------------
    #                                               LIST
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/delivery-addresses/list', methods=['GET'], cors=True)
    def customer_delivery_addresses_list():
        try:
            user_id = __get_user_id()
            return __response_list(user_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               ADD
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/delivery-addresses/add', methods=['POST'], cors=True)
    def customer_delivery_addresses_add():
        customer_storage = CustomerStorageImplementation()
        delivery_address_service = CustomerDeliveryAddressAppService(customer_storage)

        try:
            request_data = blueprint.current_request.json_body
            address_type = str(request_data.get('address_type')).strip()

            user_id = __get_user_id()
            form = __create_delivery_address_form(address_type)
            form.load(request_data)
            if form.validate():
                delivery_address_service.add_delivery_address(user_id, form)
                return __response_list(user_id)
            else:
                return {
                    'validation_errors': form.validation_errors,
                }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               EDIT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/delivery-addresses/edit', methods=['PUT'], cors=True)
    def customer_delivery_addresses_edit():
        customer_storage = CustomerStorageImplementation()
        delivery_address_service = CustomerDeliveryAddressAppService(customer_storage)

        try:
            request_data = blueprint.current_request.json_body
            address_type = str(request_data.get('address_type')).strip()
            address_hash = request_data.get('hash', None)
            address_hash = str(address_hash).strip() if address_hash is not None else None
            if not address_hash:
                raise HttpIncorrectInputDataException('"hash" parameter is incorrect!')

            user_id = __get_user_id()
            form = __create_delivery_address_form(address_type)

            form.load(request_data)
            if form.validate():
                delivery_address_service.edit_delivery_address(user_id, address_hash, form)
                return __response_list(user_id)
            else:
                return {
                    'validation_errors': form.validation_errors,
                }
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               REMOVE
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/customer/delivery-addresses/remove', methods=['DELETE'], cors=True)
    def customer_delivery_addresses_edit():
        customer_storage = CustomerStorageImplementation()
        delivery_address_service = CustomerDeliveryAddressAppService(customer_storage)

        try:
            request_data = blueprint.current_request.json_body
            address_hash = request_data.get('hash', None)
            address_hash = str(address_hash).strip() if address_hash is not None else None
            if not address_hash:
                raise HttpIncorrectInputDataException('"hash" parameter is incorrect!')

            user_id = __get_user_id()
            delivery_address_service.remove_delivery_address(user_id, address_hash)
            return __response_list(user_id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

