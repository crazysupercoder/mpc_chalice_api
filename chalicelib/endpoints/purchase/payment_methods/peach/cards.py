import re
import json
import hashlib
import datetime
import requests
from functools import reduce
from typing import Tuple, List, Optional
from chalice import Blueprint, BadRequestError, NotFoundError, UnprocessableEntityError
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.purchase.core import Id, Order
from chalicelib.libs.purchase.payment_methods.peach.cards import CreditCard, CreditCardsStorageImplementation
from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
from chalicelib.libs.purchase.order.service import OrderAppService
from chalicelib.libs.purchase.cart.service import CartAppService
from chalicelib.libs.purchase.checkout.service import CheckoutAppService
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation


class CreditCardForm(object):
    def __init__(self):
        self.number: Optional[str] = None
        self.expiry_month: Optional[str] = None
        self.expiry_year_2d: Optional[str] = None
        self.cvv: Optional[str] = None
        self.holder_name: Optional[str] = None

    def load(self, data: dict) -> None:
        def __str_or_none(s):
            return str(s).strip() if s is not None and str(s).strip() else None

        self.number = __str_or_none(data.get('number', ''))
        self.expiry_month = __str_or_none(data.get('expiry_month', ''))
        self.expiry_year_2d = __str_or_none(data.get('expiry_year_2d', ''))
        self.cvv = __str_or_none(data.get('cvv', ''))
        self.holder_name = __str_or_none(data.get('holder_name', ''))

    @property
    def brand(self) -> Optional[str]:
        # https://en.wikipedia.org/wiki/Payment_card_number
        # https://stackoverflow.com/questions/72768/how-do-you-detect-credit-card-type-based-on-number
        def detect_card_brand(number: str) -> Optional[str]:
            brands_map = {
                'visa': r'^4[0-9]{12}(?:[0-9]{3})?$',
                'electron': r'^(4026|417500|4405|4508|4844|4913|4917)\d+$',
                'mastercard': r'^5[1-5][0-9]{14}$',
                'amex': r'^3[47][0-9]{13}$',
                # 'maestro': r'^(5018|5020|5038|5612|5893|6304|6759|6761|6762|6763|0604|6390)\d+$',
                # 'dankort': r'^(5019)\d+$',
                # 'diners': r'^3(?:0[0-5]|[68][0-9])[0-9]{11}$',
                # 'discover': r'^6(?:011|5[0-9]{2})[0-9]{12}$',
                # 'jcb': r'^(?:2131|1800|35\d{3})\d{11}$',
            }
            for brand_key in tuple(brands_map.keys()):
                if re.compile(brands_map[brand_key]).match(number.replace(' ', '').strip()):
                    return brand_key.upper()

            return None

        return detect_card_brand(self.number) if self.number else None

    def validate(self) -> Tuple[dict]:
        """ [ {attribute_name: str, error_message: str}, ... ]"""
        validation_errors = []

        # number
        # https://en.wikipedia.org/wiki/Luhn_algorithm
        def luhn(code: str) -> bool:
            lookup = (0, 2, 4, 6, 8, 1, 3, 5, 7, 9)
            code = reduce(str.__add__, filter(str.isdigit, code))
            evens = sum(int(i) for i in code[-1::-2])
            odds = sum(lookup[int(i)] for i in code[-2::-2])
            return (evens + odds) % 10 == 0

        if not self.number:
            validation_errors.append({
                'attribute_name': 'number',
                'error_message': 'Attribute is required!',
            })
        elif not re.compile(r'^\d{12,19}$').match(self.number):
            validation_errors.append({
                'attribute_name': 'number',
                'error_message': 'Must be 12-19 digits length!',
            })
        elif not luhn(self.number):
            validation_errors.append({
                'attribute_name': 'number',
                'error_message': 'Value is incorrect!',
            })
        elif self.brand is None:
            validation_errors.append({
                'attribute_name': 'number',
                'error_message': 'Card Brand is not Supported!',
            })

        # expiry
        today = datetime.date.today()
        if not self.expiry_month:
            validation_errors.append({
                'attribute_name': 'expiry_month',
                'error_message': 'Attribute is required!',
            })
        elif not re.compile(r'^\d{2}$').match(self.expiry_month) and not int(self.expiry_month) in range(1, 13):
            validation_errors.append({
                'attribute_name': 'expiry_month',
                'error_message': 'Value is incorrect!',
            })
        elif not self.expiry_year_2d:
            validation_errors.append({
                'attribute_name': 'expiry_year_2d',
                'error_message': 'Attribute is required!',
            })
        elif not re.compile(r'^\d{2}$').match(self.expiry_year_2d):
            validation_errors.append({
                'attribute_name': 'expiry_year_2d',
                'error_message': 'Value is incorrect!',
            })
        elif datetime.date(
            year=2000 + int(self.expiry_year_2d),
            month=int(self.expiry_month) + 1,
            day=1
        ) - datetime.timedelta(days=1) < today:
            validation_errors.append({
                'attribute_name': 'expiry_year_2d',
                'error_message': 'Card is Expired!',
            })

        # cvv
        if not self.cvv:
            validation_errors.append({
                'attribute_name': 'cvv',
                'error_message': 'Attribute is required!',
            })
        elif not re.compile(r'^\d{3}$').match(self.cvv):
            validation_errors.append({
                'attribute_name': 'cvv',
                'error_message': 'Value is incorrect!',
            })

        # holder_name
        if self.holder_name and len(self.holder_name) > 255:
            validation_errors.append({
                'attribute_name': 'holder_name',
                'error_message': 'Value is too long!',
            })

        result: List[dict] = validation_errors
        return tuple(result)


# ----------------------------------------------------------------------------------------------------------------------


def register_payment_methods_peach_cards(blueprint: Blueprint) -> None:
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user

    def __get_card_response(card: CreditCard) -> dict:
        return {
            # We don't want to show card-api tokens, but we need to identify cards
            # between requests, so we use hashes, created from card's data.
            'id': hashlib.md5(json.dumps({
                'token': hashlib.md5(card.token.encode('utf-8')).hexdigest(),
                'brand': card.brand,
                'number_hidden': card.number_hidden,
                'expiry_month': card.expires.strftime('%m'),
                'expiry_year': card.expires.strftime('%y'),
                'holder_name': card.holder or '',
            }).encode('utf-8')).hexdigest(),
            'brand': card.brand,
            'number_hidden': card.number_hidden,
            'expiry_month': card.expires.strftime('%m'),
            'expiry_year': card.expires.strftime('%y'),
            'holder_name': card.holder,
            'is_verified': card.is_verified,
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CRUD
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/credit-cards/list', methods=['GET'], cors=True)
    def credit_cards_list():
        cards_storage = CreditCardsStorageImplementation()
        logger = Logger()

        try:
            user = __get_user()
            cards = cards_storage.get_all_by_customer(user.id)
            return [__get_card_response(card) for card in cards]
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

    @blueprint.route('/payment-methods/credit-cards/add', methods=['POST'], cors=True)
    def credit_cards_add():
        cards_storage = CreditCardsStorageImplementation()
        logger = Logger()

        try:
            user = __get_user()

            input_data = blueprint.current_request.json_body or {}
            if not input_data:
                raise BadRequestError('Incorrect Input Data!')

            redirect_back_path = input_data.get('redirect_back_path')
            if not redirect_back_path or not isinstance(redirect_back_path, str):
                raise BadRequestError('Incorrect Input Data!')

            redirect_back_path = '/' + redirect_back_path if redirect_back_path[0] != '/' else redirect_back_path

            form = CreditCardForm()
            form.load(input_data.get('form') or {})
            form_errors = form.validate()
            if form_errors:
                return {
                    'status': False,
                    'form_errors': form_errors,
                }

            def __log_flow(text: str) -> None:
                logger.log_simple('Creating Credit Card for customer #{}: {}'.format(__get_user().id, text))

            __log_flow('Start')

            try:
                __log_flow('Initial Payment...')
                response = requests.post(
                    url=settings.PEACH_PAYMENT_BASE_URL + 'payments',
                    data=(lambda data: data.update(
                        {'card.holder': form.holder_name} if form.holder_name else {}
                    ) or data)({
                        'entityId': settings.PEACH_PAYMENT_ENTITY_ID,
                        'card.number': form.number,
                        'card.expiryMonth': form.expiry_month,
                        'card.expiryYear': str(2000 + int(form.expiry_year_2d)),
                        'card.cvv': form.cvv,
                        'amount': '%.2f' % 1,
                        'currency': 'ZAR',
                        'paymentType': 'PA',
                        'recurringType': 'INITIAL',
                        'createRegistration': True,
                        'shopperResultUrl': requests.utils.requote_uri(settings.FRONTEND_BASE_URL + redirect_back_path)
                    }),
                    headers={
                        'Authorization': 'Bearer {}'.format(settings.PEACH_PAYMENT_ACCESS_TOKEN)
                    }
                )
                if response.status_code != 200:
                    raise Exception('Peach Payment Request has been Failed: {} - {} - {}'.format(
                        response.status_code,
                        response.reason,
                        response.text
                    ))

                response_data = response.json()
                if response_data['result']['code'] not in (
                    '000.200.000',  # transaction pending
                    '000.000.000',  # Transaction successfully processed in LIVE system
                    '000.100.110',  # Transaction successfully processed in TEST system
                ):
                    raise Exception('Peach Payment is not available now: {}'.format(response_data['result']))

                __log_flow('Initial Payment Done!')

                __log_flow('New Card Creation...')
                card = CreditCard(
                    response_data['registrationId'],
                    user.id,
                    form.brand,
                    ('*' * len(form.number[0:-4])) + form.number[-4:],
                    datetime.date(
                        year=2000 + int(form.expiry_year_2d),
                        month=int(form.expiry_month),
                        day=31
                    ) if int(form.expiry_month) == 12 else (datetime.date(
                        year=2000 + int(form.expiry_year_2d),
                        month=int(form.expiry_month) + 1,
                        day=1
                    ) - datetime.timedelta(days=1)),
                    form.holder_name
                )
                __log_flow('New Card Created {}. Saving...'.format(card.token))
                cards_storage.save(card)
                __log_flow('New Card Saved!')

                __log_flow('Initial Payment Done!')

                result = {
                    'url': response_data['redirect']['url'],
                    'method': 'POST',
                    'parameters': response_data['redirect']['parameters'],
                }

                __log_flow('End')

                return result
            except BaseException as e:
                __log_flow('Fail because of Error {}'.format(str(e)))
                raise e
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

    @blueprint.route('/payment-methods/credit-cards/remove', methods=['DELETE'], cors=True)
    def credit_cards_remove():
        cards_storage = CreditCardsStorageImplementation()
        logger = Logger()

        try:
            user = __get_user()

            card_response_id = (blueprint.current_request.json_body or {}).get('card_id') or None
            if not card_response_id:
                raise BadRequestError('Incorrect Input Data!')

            card = None
            for _card in cards_storage.get_all_by_customer(user.id):
                if card_response_id == __get_card_response(_card)['id']:
                    card = _card
                    break

            if not card:
                raise NotFoundError('Card not Found!')

            def __log_flow(text: str) -> None:
                logger.log_simple('Removing Credit Card #{} : {}'.format(card.token, text))

            __log_flow('Start')

            try:
                __log_flow('Removing from storage...')
                cards_storage.remove(card)
                __log_flow('Removed from storage!')

                __log_flow('Removing from Peach...')
                response = requests.delete(
                    url=settings.PEACH_PAYMENT_BASE_URL + 'registrations/{}?entityId={}'.format(
                        card.token,
                        settings.PEACH_PAYMENT_ENTITY_ID
                    ),
                    headers={
                        'Authorization': 'Bearer {}'.format(settings.PEACH_PAYMENT_ACCESS_TOKEN)
                    })

                if response.status_code != 200:
                    raise Exception('Peach Payment Request has been Failed: {} - {} - {}'.format(
                        response.status_code,
                        response.reason,
                        response.text
                    ))

                response_data = response.json()
                if response_data['result']['code'] not in (
                    '000.000.000',  # Transaction successfully processed in LIVE system
                    '000.100.110',  # Transaction successfully processed in TEST system
                ):
                    raise Exception('Peach Payment is not available now: {}'.format(response_data['result']))

                __log_flow('Removed from Peach!')
            except BaseException as e:
                __log_flow('Not done because of Error: {}'.format(str(e)))
                raise e

            __log_flow('End')
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

        return {
            'Code': 'Success',
            'Message': 'Success',
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CHECKOUT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/credit-cards/checkout', methods=['POST'], cors=True)
    def credit_cards_checkout():
        cards_storage = CreditCardsStorageImplementation()
        checkout_storage = CheckoutStorageImplementation()
        order_app_service = OrderAppService()
        cart_service = CartAppService()
        checkout_service = CheckoutAppService()
        logger = Logger()

        try:
            user = __get_user()

            card_response_id = (blueprint.current_request.json_body or {}).get('card_id') or None
            if not card_response_id:
                raise BadRequestError('Incorrect Input Data!')

            card = None
            for _card in cards_storage.get_all_by_customer(user.id):
                if __get_card_response(_card)['id'] == card_response_id:
                    card = _card
                    break

            if not card:
                raise NotFoundError('Card does not exist!')
            elif not card.is_verified:
                raise ApplicationLogicException('Unable to checkout with Not Verified Card!')

            checkout = checkout_storage.load(Id(user.id))
            if not checkout:
                raise HttpNotFoundException('Checkout does not exist!')
            elif checkout.total_due.value == 0:
                raise ApplicationLogicException('Unable to checkout 0 amount with Credit Cards!')

            order = order_app_service.get_waiting_for_payment_by_checkout_or_checkout_new(user.id)

            def __log_flow(text: str) -> None:
                logger.log_simple('Credit Cards : Checkout : {} : {}'.format(order.number.value, text))

            __log_flow('Start')

            # init
            try:
                __log_flow('Payment Request...')
                response = requests.post(
                    url=settings.PEACH_PAYMENT_BASE_URL + 'registrations/{}/payments'.format(card.token),
                    data={
                        'entityId': settings.PEACH_PAYMENT_ENTITY_ID,
                        'amount': '%.2f' % order.total_current_cost_ordered.value,
                        'paymentType': 'DB',
                        'currency': 'ZAR',
                        'shopperResultUrl': requests.utils.requote_uri(
                            settings.FRONTEND_BASE_URL + '/order/confirmation/{}'.format(order.number.value)
                        ),
                        'customParameters[order_number]': order.number.value,
                    },
                    headers={
                        'Authorization': 'Bearer {}'.format(settings.PEACH_PAYMENT_ACCESS_TOKEN)
                    }
                )
                if response.status_code != 200:
                    raise Exception('Peach Payment Request has been Failed: {} - {} - {}'.format(
                        response.status_code,
                        response.reason,
                        response.text
                    ))

                response_data = response.json()
                if response_data['result']['code'] not in (
                    '000.200.000',  # transaction pending
                ):
                    raise Exception('Credit Card Initial request response is not good: {} - {}'.format(
                        response_data['result']['code'],
                        response_data['result']['description']
                    ))

                __log_flow('Payment Request is Done!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Payment Request is Not done because of Error: {}'.format(str(e)))
                raise UnprocessableEntityError('Credit Card Payment is unavailable now!')

            # flush cart (silently)
            try:
                __log_flow('Cart Flushing...')
                cart_service.clear_cart(user.session_id)
                __log_flow('Cart Flushed!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Cart is NOT Flushed because of Error: {}'.format(str(e)))

            # flush checkout (silently)
            try:
                __log_flow('Checkout Flushing...')
                checkout_service.remove(user.id)
                __log_flow('Checkout Flushed!')
            except BaseException as e:
                logger.log_exception(e)
                __log_flow('Checkout is NOT Flushed because of Error: {}'.format(str(e)))

            result = {
                'order_number': order.number.value,
                'url': response_data['redirect']['url'],
                'method': 'POST',
                'parameters': [{
                    'name': param['name'],
                    'value': param['value'],
                } for param in response_data['redirect']['parameters']]
            }

            __log_flow('End')

            return result
        except BaseException as e:
            logger.log_exception(e)
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                     SHOPPER RESULT - CHECKOUT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/credit-cards/shopper-result', methods=['GET'], cors=True)
    def credit_cards_shopper_result():
        # Attention! All payment logic is done in webhooks!

        # Theoretically we can just get the last order by customer,
        # because currently it's hard for user to do some tricks here,
        # but event in this case user just gets info about his another order.

        order_storage = OrderStorageImplementation()

        try:
            user = __get_user()

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

        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ----------------------------------------------------------------------------------------------------------------------


