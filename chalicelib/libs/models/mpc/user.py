import boto3
from typing import List, Optional
from warnings import warn
from chalicelib.settings import settings
from chalicelib.extensions import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.models.mpc.Cms.user_states import CustomerStateModel, CustomerStateEntry
from .Cms.profiles import Profile, UserQuestionModel, USER_QUESTION_TYPE
from ....constants.sqs import SCORED_PRODUCT_MESSAGE_TYPE


class ScoredProductSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, event_type: str, email: str = None):
        self.__event_type = event_type
        self.__email = email

    @property
    def event_type(self) -> str:
        return self.__event_type

    @property
    def event_data(self) -> dict:
        return {
            'email': self.__email
        }


# ----------------------------------------------------------------------------------------------------------------------


class UserAnswerSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, id, email, answer):
        self.__id = id
        self.__email = email
        self.__answer = answer

    @classmethod
    def _get_event_type(cls) -> str:
        return 'user_answer'

    @property
    def event_data(self) -> dict:
        answer = clone(self.__answer)
        answer['user'] = {
            'id': self.__id,
            'email': self.__email
        }
        return answer

# ----------------------------------------------------------------------------------------------------------------------

class User(object):
    COGNITO_USER_POOL_ID = settings.AWS_COGNITO_USER_POOL_ID
    DEFAULT_EMAIL = 'BLANK'
    DEFAULT_GENDER = 'UNISEX'
    __id = None
    __session_id = None
    __data = None
    __profile = None
    __client = boto3.client('cognito-idp')
    __email = None
    __first_name = None
    __last_name = None
    __state__: CustomerStateEntry = None

    def __init__(self, session_id, id=None, email=None, first_name=None, last_name=None, **kwargs):
        self.session_id = session_id
        if id:
            self.__id = id
            self.__email = email
            self.__first_name = first_name
            self.__last_name = last_name
        self.profile = Profile(
            session_id, customer_id=id,
            email=self.email)

    @property
    def data(self) -> dict:
        if not isinstance(self.__data, dict):
            self.__data = self.__class__.find_user(self.id)
        return self.__data

    @property
    def cognito_client(self):
        return self.__client

    @property
    def session_id(self):
        return self.__session_id

    @property
    def user_id(self):
        return self.__id

    @session_id.setter
    def session_id(self, value):
        self.__session_id = value

    @property
    def first_name(self):
        return self.__first_name

    @property
    def last_name(self):
        return self.__last_name

    @property
    def state(self) -> Optional[CustomerStateEntry]:
        if not self.id:
            return None
        if not isinstance(self.__state__, CustomerStateEntry):
            self.__state__ = CustomerStateModel(self.id, self.email).state
        return self.__state__

    @property
    def is_personalized(self) -> bool:
        if isinstance(self.state, CustomerStateEntry):
            return self.state.is_personalized
        else:
            return False

    @property
    def email(self):
        if self.__email:
            return self.__email

        if self.data:
            for item in self.data.get('UserAttributes', []):
                if item.get('Name') == 'email':
                    return item.get('Value', self.DEFAULT_EMAIL)
        return self.DEFAULT_EMAIL

    @property
    def id(self):
        return self.__id

    @property
    def is_anyonimous(self):
        return self.id is None

    @property
    def profile(self) -> Profile:
        if self.__profile is None:
            return Profile(self.session_id, self.id)
        else:
            return self.__profile

    @profile.setter
    def profile(self, value: Profile):
        self.__profile = value

    @property
    def questions(self):
        return self.profile.questions

    @property
    def gender(self):
        if self.data is not None:
            for item in self.data.get('UserAttributes', []):
                if item.get('Name') in ['gender', 'custom:gender']:
                    return item.get('Value', self.DEFAULT_GENDER)
        elif self.profile.gender is not None:
            return self.profile.gender
        else:
            return 'LADIES'

    @property
    def groups(self) -> List[str]:
        try:
            response = self.cognito_client.admin_list_groups_for_user(
                Username=self.id,
                UserPoolId=self.COGNITO_USER_POOL_ID,
                Limit=10
            )
            return [item.get('GroupName', '').lower() for item in response.get('Groups', [])]
        except Exception as e:
            return []

    @property
    def is_admin(self) -> bool:
        groups = self.groups
        return 'admin' in groups

    @classmethod
    def find_user(cls, id):
        try:
            client = boto3.client('cognito-idp')
            return client.admin_get_user(
                UserPoolId=cls.COGNITO_USER_POOL_ID,
                Username=id
            )
        except:
            return None

    @classmethod
    def send_calculate_product_score_for_customers(cls, emails: List[str] = None) -> bool:
        if emails is None:
            emails = cls.get_all_emails()
        events = list()
        for email in emails:
            events.append(ScoredProductSqsSenderEvent(
                SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER,
                email=email))
        SqsSenderImplementation().send_batch(events)

    @classmethod
    def get_customer_attributes(cls, attrs: List[str]) -> List[dict]:
        try:
            client = boto3.client('cognito-idp')
            response = cls.__client.list_users(
                UserPoolId=cls.COGNITO_USER_POOL_ID,
                AttributesToGet=attrs,
            )
            results = list()
            for user in response.get('Users'):
                buffer = dict()
                for item in user['Attributes']:
                    if item['Name'] in attrs:
                        buffer.update({item['Name']: item['Value']})
                results.append(buffer)
            return results
        except Exception as e:
            warn(str(e))
            return []

    @classmethod
    def get_all_emails(cls) -> List[str]:
        items = cls.get_customer_attributes(['email'])
        return [item['email'] for item in items]

    @classmethod
    def get_username_with_email(cls, email: str) -> str:
        response = cls.__client.list_users(
            UserPoolId=cls.COGNITO_USER_POOL_ID,
            AttributesToGet=['sub'],
            Limit=1,
            Filter="email=\"%s\"" % email
        )
        users = response.get('Users', [])
        if len(users) == 0:
            return None
        else:
            return users[0].get('Username')

    @classmethod
    def admin_set_user_password(
            cls,
            password: str,
            username: str = None,
            email: str = None,
            permanent: bool = True):
        if username is None:
            if email is None:
                return False
            else:
                username = cls.get_username_with_email(email)
                if username is None:
                    return False

        try:
            response = cls.__client.admin_set_user_password(
                UserPoolId=cls.COGNITO_USER_POOL_ID,
                Username=username,
                Password=password,
                Permanent=permanent)
            return True
        except Exception as e:
            return False

    def get_question(self, id):
        question_model = UserQuestionModel()
        if self.is_anyonimous:
            return question_model.get_item(id)
        else:
            item = self.profile.get_answer(id)
            if item is None:
                item = question_model.get_item(id)
            return item

    def manage_answer(self, body):
        attr = body['attribute']
        if attr['type'] == USER_QUESTION_TYPE.customer:
            if attr['value'] == 'gender':
                answer = body.get('answer', [''])[0]
                if not answer:
                    return False
                options = [option for option in body['options'] if option['id'] == answer]
                if len(options) == 0:
                    return False

                value = options[0].get('value', '')
                if value.lower() in ['male', 'men', 'mens']:
                    return self.set_gender('mens')
                elif value.lower() in ['lady', 'female', 'ladies']:
                    return self.set_gender('ladies')
                else:
                    return self.set_gender()
            elif attr['value'] == 'name':
                answer = body.get('answer', [''])[0]
                if not answer:
                    return False
                return self.set_name(answer)
        elif attr['type'] == USER_QUESTION_TYPE.product:
            ids = body.get('answer', [])
            answer = [
                option['value'] for option in body.get('options', [])
                if option['id'] in ids]
            if attr['value'] == 'brand':
                self.profile.set_brands(answer)
            elif attr['value'] == 'producttype':
                self.profile.set_product_types(answer)

    def set_gender(self, gender='UNISEX'):
        if self.is_anyonimous or self.__email is not None:
            return self.profile.set_gender(gender)
        else:
            return self.cognito_client.admin_update_user_attributes(
                UserPoolId=self.COGNITO_USER_POOL_ID,
                Username=self.id,
                UserAttributes=[
                    {
                        'Name': 'gender',
                        'Value': gender.upper()
                    }
                ]
            )

    def set_name(self, name):
        if self.is_anyonimous or self.__email is not None:
            return self.profile.set_name(name)
        else:
            return self.cognito_client.admin_update_user_attributes(
                UserPoolId=self.COGNITO_USER_POOL_ID,
                Username=self.id,
                UserAttributes=[
                    {
                        'Name': 'name',
                        'Value': name
                    }
                ]
            )

    def save_answer(self, question_id, body):
        # Process answers in customer level
        response = self.manage_answer(body)

        # Save answers to dynamoDB
        if self.profile.save_answer(question_id, body):
            # Send answer to SQS for Portal
            self.send_answer_to_sqs(body)
            self.__class__.send_calculate_product_score_for_customers(emails=[self.email])
            return True
        return False

    def send_answer_to_sqs(self, answer) -> None:
        if self.is_anyonimous:
            return

        event = UserAnswerSqsSenderEvent(self.id, self.email, answer)
        SqsSenderImplementation().send(event)

    def sync_user_attributes(self, attributes, **kwargs):
        if self.is_anyonimous:
            return False

        if self.__first_name is not None and self.__last_name is not None:
            self.set_name(self.__first_name + self.__last_name)
        else:
            if attributes.get('name'):
                self.set_name(attributes.get('name'))

            if attributes.get('gender'):
                self.set_gender(attributes.get('gender'))

        return True
