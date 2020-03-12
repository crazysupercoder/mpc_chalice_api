import re
from typing import Optional, List
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.core.chalice.request import MPCRequest
from chalicelib.libs.core.logger import Logger
from chalice import UnauthorizedError
from chalicelib.libs.informations.sqs import InformationsSqsSenderEvent
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.models.mpc.Cms.Informations import InformationService
from chalicelib.libs.models.ml.scored_products import ScoredProduct

# @todo : move this form somewhere
class AccountInformationForm(object):
    ATTRIBUTE_FIRST_NAME = 'first_name'
    ATTRIBUTE_LAST_NAME = 'last_name'
    ATTRIBUTE_EMAIL = 'email'
    ATTRIBUTE_GENDER = 'gender'
    ATTRIBUTE_IDENTIFICATION_NUMBER = 'identification_number'

    ATTRIBUTES_MAP = {
        ATTRIBUTE_FIRST_NAME: 'First Name',
        ATTRIBUTE_LAST_NAME: 'Last Name',
        ATTRIBUTE_GENDER: 'Gender',
        ATTRIBUTE_EMAIL: 'Email',
        ATTRIBUTE_IDENTIFICATION_NUMBER: 'ID',
    }

    __IDENTIFICATION_NUMBER_MIN_LENGTH = 10
    __IDENTIFICATION_NUMBER_MAX_LENGTH = 19
    __GENDERS = ['male', 'female']

    def __init__(self) -> None:
        self.__first_name = None
        self.__last_name = None
        self.__email = None
        self.__gender = None
        self.__identification_number = None

    @property
    def first_name(self) -> Optional[str]:
        return self.__first_name

    @property
    def last_name(self) -> Optional[str]:
        return self.__last_name

    @property
    def gender(self) -> Optional[str]:
        return self.__gender

    @property
    def email(self) -> Optional[str]:
        return self.__email

    @property
    def identification_number(self) -> Optional[str]:
        return self.__identification_number

    def load(self, data: dict) -> None:
        self.__first_name = str(data.get(self.__class__.ATTRIBUTE_FIRST_NAME, None) or '').strip() or None
        self.__last_name = str(data.get(self.__class__.ATTRIBUTE_LAST_NAME, None) or '').strip() or None
        self.__gender = str(data.get(self.__class__.ATTRIBUTE_GENDER, None) or '').strip() or None
        self.__email = str(data.get(self.__class__.ATTRIBUTE_EMAIL, None) or '').strip() or None
        self.__identification_number = str(data.get(self.__class__.ATTRIBUTE_IDENTIFICATION_NUMBER, None) or '').strip() or None

    def validate(self) -> dict:
        """:return: {attribute_name: [error_message, ...], ...}"""
        errors = {}

        # required
        required_attributes = [
            self.__class__.ATTRIBUTE_FIRST_NAME,
            self.__class__.ATTRIBUTE_LAST_NAME,
            self.__class__.ATTRIBUTE_GENDER,
            self.__class__.ATTRIBUTE_EMAIL,
        ]
        for attribute_name in required_attributes:
            if getattr(self, attribute_name) is None:
                errors[attribute_name]: List = errors.get(attribute_name) or []
                errors[attribute_name].append('"{}" is required'.format(self.__class__.ATTRIBUTES_MAP[attribute_name]))

        # email
        if self.email and not re.match(r'[^@]+@[^@]+\.[^@]+', self.email):
            attribute_name = self.__class__.ATTRIBUTE_EMAIL
            errors[attribute_name]: List = errors.get(attribute_name) or []
            errors[attribute_name].append('"{}" is not Email'.format(self.__class__.ATTRIBUTES_MAP[attribute_name]))

        # gender
        if self.gender and self.gender not in self.__class__.__GENDERS:
            attribute_name = self.__class__.ATTRIBUTE_GENDER
            errors[attribute_name]: List = errors.get(attribute_name) or []
            errors[attribute_name].append('"{}" is unknown'.format(self.__class__.ATTRIBUTES_MAP[attribute_name]))

        # identification number
        if self.identification_number and \
            (len(self.identification_number) < self.__class__.__IDENTIFICATION_NUMBER_MIN_LENGTH or len(self.identification_number) > self.__class__.__IDENTIFICATION_NUMBER_MAX_LENGTH):
            attribute_name = self.__class__.ATTRIBUTE_IDENTIFICATION_NUMBER
            errors[attribute_name]: List = errors.get(attribute_name) or []
            errors[attribute_name].append('The length of "{}" number must be between {} and {}'.format(
                self.__class__.ATTRIBUTES_MAP[attribute_name],
                self.__class__.__IDENTIFICATION_NUMBER_MIN_LENGTH,
                self.__class__.__IDENTIFICATION_NUMBER_MAX_LENGTH
            ))

        return errors


# ----------------------------------------------------------------------------------------------------------------------


def register_informations(blue_print):
    def __get_request() -> MPCRequest:
        return blue_print.current_request

    def __get_current_user() -> User:
        user = __get_request().current_user
        if user.is_anyonimous:
            raise UnauthorizedError('Authentication is required!')

        return user

    @blue_print.route('/informations', methods=['GET', 'POST'], cors=True)
    def informations():
        logger = Logger()
        user = __get_current_user()
        request = __get_request()
        if request.method == 'GET':
            user_info = user.profile.informations
            if user_info['email'] == None or user_info['email'] == '' or user_info['email'] == 'BLANK':
                user_info['email'] = user.email
            if user_info['first_name'] == None or user_info['first_name'] == '' and user.first_name is not None:
                user_info['first_name'] = user.first_name
            if user_info['last_name'] == None or user_info['last_name'] == '' and user.last_name is not None:
                user_info['last_name'] = user.last_name
            user.profile.informations = user_info
            return user_info
        elif request.method == 'POST':
            params = request.json_body
            try:
                form = AccountInformationForm()
                form.load(params)
                validation_errors = form.validate()
                if validation_errors:
                    return {'status': False, 'msg': tuple(validation_errors.values())[0][0]}

                user.profile.informations = params

                event = InformationsSqsSenderEvent(user.profile.informations)
                SqsSenderImplementation().send(event)
                return {'status': True, 'msg': 'Information has been updated successfully'}
            except Exception as e:
                logger.log_exception(e)
                return {'status': False, 'msg': str(e)}

    @blue_print.route('/informations/tier', methods=['GET'], cors=True)
    def tier():
        user = __get_current_user()
        return {"status": True, "tier": user.profile.tier}

    @blue_print.route('/add-information', methods=['POST'], cors=True)
    def add_information():
        logger = Logger()
        user = __get_current_user()
        request = __get_request()
        params = request.json_body
        try:
            user.profile.add_information(params)
            event = InformationsSqsSenderEvent(user.profile.informations)
            SqsSenderImplementation().send(event)
            return {'status': True}
        except Exception as e:
            logger.log_exception(e)
            return {'status': False, 'msg': str(e)}

    @blue_print.route('/addresses', methods=['GET', 'POST'], cors=True)
    def addresses():
        logger = Logger()
        user = __get_current_user()
        request = __get_request()
        if request.method == 'GET':
            return user.profile.informations['addresses']
        elif request.method == 'POST':
            params = request.json_body
            try:
                ret = user.profile.add_addresses(params)
                event = InformationsSqsSenderEvent(user.profile.informations)
                SqsSenderImplementation().send(event)
                return {'status': True, 'data': ret["Attributes"]["addresses"]}
            except Exception as e:
                logger.log_exception(e)
                return {'status': False, 'msg': str(e)}

    @blue_print.route('/add-address', methods=['POST'], cors=True)
    def addaddress():
        user = __get_current_user()
        request = __get_request()
        param = request.json_body
        try:
            user.profile.add_address(param)
            event = InformationsSqsSenderEvent(user.profile.informations)
            SqsSenderImplementation().send(event)
            ret = user.profile.informations['addresses']
            return {'status': True, 'data': ret}
        except Exception as e:
            return {'status': False, 'msg': str(e)}

    @blue_print.route('/get-address/{address_hash}', methods=['GET'], cors=True)
    def getaddress(address_hash):
        logger = Logger()
        user = __get_current_user()
        try:
            ret = user.profile.get_address(address_hash)
            return {'status': True, 'data': ret}
        except Exception as e:
            logger.log_exception(e)
            return {'status': False, 'msg': str(e)}

    @blue_print.route('/delete-address/{address_hash}', methods=['DELETE'], cors=True)
    def deleteaddress(address_hash):
        logger = Logger()
        user = __get_current_user()
        try:
            user.profile.delete_address(address_hash)
            event = InformationsSqsSenderEvent(user.profile.informations)
            SqsSenderImplementation().send(event)
            ret = user.profile.informations['addresses']
            return {'status': True, 'data': ret}
        except Exception as e:
            logger.log_exception(e)
            return {'status': False, 'msg': str(e)}

    # @todo : this is a crutch
    # Currently Sign-Up process ignores backend and goes to the cognito directly, so we can't control input data.
    # Identification Number (or ID) is not required field, but it should be set inside Sign-Up process.
    # Currently /add-information endpoint requires and updates all account data - that is why this endpoint was created.
    # Email is also required to be saved in the database after sign-up, at least because user is able to do
    # purchases without populating of account information, but email is required for orders.
    @blue_print.route('/set-on-sign-up', methods=['PUT'], cors=True)
    def on_signup():
        logger = Logger()
        user = __get_current_user()

        # Attention! Please, do all actions silently (in try-except block)
        try:
            # Trigger to calculate product scores per the just registerred customer here.
            pass
        except Exception as e:
            pass

        # subscription
        try:
            __on_signup__subscription(user)
        except BaseException as e:
            logger.log_exception(e)

        # request customer info
        try:
            __on_signup_request_customer_info(user)
        except BaseException as e:
            logger.log_exception(e)

        # set information email and identification number
        try:
            email_value = str(__get_request().json_body.get('email') or '').strip()
            identification_number_value = str(__get_request().json_body.get('identification_number') or '').strip()

            # this is not usual usage, but it's simple to check identification number and email here in this way
            form = AccountInformationForm()
            form.load({
                AccountInformationForm.ATTRIBUTE_EMAIL: email_value,
                AccountInformationForm.ATTRIBUTE_IDENTIFICATION_NUMBER: identification_number_value
            })
            form_errors = form.validate()
            email_errors = form_errors.get(AccountInformationForm.ATTRIBUTE_EMAIL, [])
            identification_number_errors = form_errors.get(AccountInformationForm.ATTRIBUTE_IDENTIFICATION_NUMBER, [])
            if email_errors or identification_number_errors:
                return {'status': False, 'message': (email_errors + identification_number_errors)[0]}

            # update only identification number and email
            information = user.profile.informations
            information['email'] = email_value
            information['identification_number'] = identification_number_value
            user.profile.informations = information

            event = InformationsSqsSenderEvent(user.profile.informations)
            SqsSenderImplementation().send(event)
            return {'status': True}
        except BaseException as e:
            logger.log_exception(e)
            return {'status': False, 'message': 'Internal Server Error'}

    def __on_signup__subscription(user: User):
        from chalicelib.libs.subscription.subscription import Id, Email, SubscriptionStorage
        subscription_storage = SubscriptionStorage()

        subscription = subscription_storage.get_by_email(Email(user.email))
        if not subscription:
            return

        subscription.assign_to_user(Id(user.id))
        subscription_storage.save(subscription)

    def __on_signup_request_customer_info(user: User):
        # @todo : this is a crutch
        # This should be a api request, but it is impossible for now,
        # so we use sqs-communication.

        from chalicelib.libs.purchase.customer.sqs import CrutchCustomerInfoRequestSqsSenderEvent
        event = CrutchCustomerInfoRequestSqsSenderEvent(user.email)
        SqsSenderImplementation().send(event)


    #This is used for test.
    @blue_print.route('/get_information_by_email', methods=['POST'], cors=True)
    def getInformation():
        logger = Logger()
        try:
            email = str(__get_request().json_body.get('email') or '').strip()
            if not email:
                raise ValueError('email is empty') 
            ret = InformationService().get(email).get_item()
            return {'status': True, 'data': ret}
        except Exception as e:
            logger.log_exception(e)
            return {'status': False, 'msg': str(e)}