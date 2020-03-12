import re
import requests
from typing import Optional, List
from chalice import Blueprint, UnprocessableEntityError, UnauthorizedError
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.purchase.core import Id, Order
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.order.service import OrderAppService
from chalicelib.libs.purchase.cart.service import CartAppService
from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
from chalicelib.libs.purchase.checkout.service import CheckoutAppService


class MobicredCredentialsForm(object):
    ATTRIBUTE_USERNAME = 'username'
    ATTRIBUTE_PASSWORD = 'password'

    __ATTRIBUTES_MAP = {
        ATTRIBUTE_USERNAME: 'Username',
        ATTRIBUTE_PASSWORD: 'Password',
    }

    def __init__(self) -> None:
        self.__username = None
        self.__password = None

    @property
    def username(self) -> Optional[str]:
        return self.__username

    @property
    def password(self) -> Optional[str]:
        return self.__password

    def load(self, data: dict) -> None:
        self.__username = str(data.get(self.__class__.ATTRIBUTE_USERNAME, None) or '').strip() or None
        self.__password = str(data.get(self.__class__.ATTRIBUTE_PASSWORD, None) or '').strip() or None

    def validate(self) -> dict:
        """:return: {attribute_name: [error_message, ...], ...}"""
        errors = {}

        for attribute_name, attribute_label in tuple(self.__class__.__ATTRIBUTES_MAP.items()):
            value = getattr(self, attribute_name)
            if not value:
                errors[attribute_name]: List = errors.get(attribute_name) or []
                errors[attribute_name].append('"{}" is required'.format(attribute_label))
            elif len(re.findall(r'[\s\S]{1,100}', value)) != 1:
                errors[attribute_name]: List = errors.get(attribute_name) or []
                errors[attribute_name].append('"{}" must be 1-100 characters length'.format(attribute_label))

        return errors


# ----------------------------------------------------------------------------------------------------------------------


def register_payment_methods_peach_mobicred(blueprint: Blueprint) -> None:
    @blueprint.route('/payment-methods/mobicred/checkout', methods=['POST'], cors=True)
    def mobicred_checkout():
        order_app_service = OrderAppService()
        cart_service = CartAppService()
        checkout_storage = CheckoutStorageImplementation()
        checkout_service = CheckoutAppService()
        logger = Logger()

        try:
            user = blueprint.current_request.current_user
            if user.is_anyonimous:
                raise HttpAuthenticationRequiredException()

            # @todo : refactoring
            checkout = checkout_storage.load(Id(user.id))
            if not checkout:
                raise HttpNotFoundException('Checkout does not exist!')
            elif checkout.total_due.value == 0:
                raise ApplicationLogicException('Unable to checkout 0 amount with Mobicred!')

            # check input data
            form = MobicredCredentialsForm()
            form.load(blueprint.current_request.json_body or {})
            form_errors = form.validate()
            if form_errors:
                return {
                    'status': False,
                    'form_errors': form_errors
                }

            order = order_app_service.get_waiting_for_payment_by_checkout_or_checkout_new(user.id)

            def __log_flow(text: str) -> None:
                logger.log_simple('Mobicred Payment Log for Order #{} : {}'.format(
                    order.number.value,
                    text
                ))

            __log_flow('Start')

            # init mobicred
            try:
                __log_flow('Payment Initializing...')
                response = requests.post(
                    url=settings.PEACH_PAYMENT_BASE_URL + 'payments​',
                    data={
                        'entityId': settings.PEACH_PAYMENT_ENTITY_ID,
                        'paymentBrand': 'MOBICRED',
                        'paymentType': 'DB',
                        'virtualAccount[accountId]': form.username,
                        'virtualAccount[password]': form.password,
                        'amount': '%.2f' % order.total_current_cost_ordered.value,
                        'currency': 'ZAR',
                        'customParameters[order_number]': order.number.value,
                        'shopperResultUrl': requests.utils.requote_uri(
                            settings.FRONTEND_BASE_URL + '/order/confirmation/{}'.format(order.number.value)
                        ),
                    },
                    headers={
                        'Authorization': 'Bearer {}'.format(settings.PEACH_PAYMENT_ACCESS_TOKEN)
                    }
                )
                if response.status_code != 200:
                    raise Exception('Mobicred Initial request has been failed: {} - {} - {}!'.format(
                        response.status_code,
                        response.reason,
                        response.text
                    ))

                response_data = response.json()
                if response_data['result']['code'] not in (
                    '000.000.000',  # Transaction successfully processed in LIVE system
                    '000.100.110',  # Transaction successfully processed in TEST system
                ):
                    raise Exception('Mobicred Initial request response is not good: {} - {}'.format(
                        response_data['result']['code'],
                        response_data['result']['description']
                    ))
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Payment Not Initialized because of Error: {}'.format(str(e)))
                raise HttpNotFoundException('Mobicred Payment is unavailable now!')

            __log_flow('Payment Initialized!')

            # flush cart (silently)
            try:
                __log_flow('Cart Flushing...')
                cart_service.clear_cart(user.session_id)
                __log_flow('Cart Flushed!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Cart Not Flushed because of Error {}!'.format(str(e)))

            # flush checkout (silently)
            try:
                __log_flow('Checkout Flushing...')
                checkout_service.remove(user.id)
                __log_flow('Checkout Flushed!')
            except BaseException as e:
                __log_flow('Cart Not Flushed because of Error {}!'.format(str(e)))
                logger.log_exception(e)

            __log_flow('End')

            result = {
                'order_number': order.number.value,
                'url': response_data['redirect']['url'],
                'method': 'POST',
                'parameters': response_data['redirect']['parameters'],
            }

            return result
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               SHOPPER RESULT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/mobicred/shopper-result', methods=['GET'], cors=True)
    def mobicred_shopper_result():
        """
        https://support.peachpayments.com/hc/en-us/articles/360026704471-Mobicred-integration-guide
        mobicred_api_resource_path = str(blueprint.current_request.get_query_parameter('resourcePath') or '').strip()
        """

        # Attention! All payment logic is done in webhooks!

        # Theoretically we can just get the last order by customer,
        # because currently it's hard for user to do some tricks here,
        # but event in this case user just gets info about his another order.

        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise UnauthorizedError('Authentication is required!')

        order_storage = OrderStorageImplementation()

        last_order: Optional[Order] = None
        orders = order_storage.get_all_for_customer(Id(user.id))
        for order in orders:
            if not last_order or order.created_at > last_order.created_at:
                last_order = order

        if not last_order:
            raise UnprocessableEntityError('No orders - something wrong!')

        return {
            'order_number': last_order.number.value
        }

# ----------------------------------------------------------------------------------------------------------------------

