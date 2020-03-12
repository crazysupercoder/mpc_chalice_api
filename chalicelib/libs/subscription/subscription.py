import uuid
from typing import Optional
from datetime import datetime
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderInterface


# ----------------------------------------------------------------------------------------------------------------------

# @todo : this is a "crutch" to use the same code for both modules. Needs to be refactored.
from chalicelib.libs.purchase.core import Id as _Id, Email as _Email


class Id(object):
    def __init__(self, value: str) -> None:
        self.__value = _Id(value).value

    @property
    def value(self) -> str:
        return self.__value


class Email(object):
    def __init__(self, value: str) -> None:
        self.__value = _Email(value).value

    @property
    def value(self) -> str:
        return self.__value


# ----------------------------------------------------------------------------------------------------------------------


class Subscription:
    def __init__(self, subscription_id: Id, email: Email):
        if not isinstance(subscription_id, Id):
            raise ArgumentTypeException(self.__init__, 'subscription_id', subscription_id)

        if not isinstance(email, Email):
            raise ArgumentTypeException(self.__init__, 'email', email)

        # Theoretically email can replace subscription_id here, but in this case
        # we need to store (email, subscription_key) pairs somewhere else
        # to be able to unsubscribe by unique url without email usage in it.
        # Usage of the user_id here has a similar sense, because we need to know,
        # which users are subscribed (e.g. for discounts) regardless their emails changing.
        # So, this mix is storage-based simplification.
        self.__subscription_id = subscription_id
        self.__email = email
        self.__user_id = None

        self.__subscribed_at = datetime.now()
        self.__unsubscribed_at = None

    @property
    def subscription_id(self) -> Id:
        return self.__subscription_id

    @property
    def email(self) -> Email:
        return self.__email

    @property
    def user_id(self) -> Optional[Id]:
        return self.__user_id

    @property
    def is_subscribed(self) -> bool:
        return self.__unsubscribed_at is None

    @property
    def is_unsubscribed(self) -> bool:
        return not self.is_subscribed

    @property
    def subscribed_at(self) -> datetime:
        # datetime objects are already immutable
        return self.__subscribed_at

    @property
    def unsubscribed_at(self) -> Optional[datetime]:
        # datetime objects are already immutable
        return self.__unsubscribed_at

    def assign_to_user(self, user_id: Id) -> None:
        if not isinstance(user_id, Id):
            raise ArgumentTypeException(self.assign_to_user, 'user_id', user_id)

        if self.__user_id is not None:
            raise ApplicationLogicException('Unable to re-assign User to Subscription!')

        self.__user_id = user_id

    def mark_subscribed(self) -> None:
        if self.is_subscribed:
            raise ApplicationLogicException('{} is already Subscribed!'.format(self.__email.value))

        self.__subscribed_at = datetime.now()
        self.__unsubscribed_at = None

    def mark_unsubscribe(self) -> None:
        if not self.is_subscribed:
            raise ApplicationLogicException('{} is already Unsubscribed!'.format(self.__email.value))

        self.__unsubscribed_at = datetime.now()


# ----------------------------------------------------------------------------------------------------------------------


class SubscriptionStorage:
    __ENTITY_PROPERTY_SUBSCRIPTION_ID = '__subscription_id'
    __ENTITY_PROPERTY_EMAIL = '__email'
    __ENTITY_PROPERTY_USER_ID = '__user_id'
    __ENTITY_PROPERTY_SUBSCRIBED_AT = '__subscribed_at'
    __ENTITY_PROPERTY_UNSUBSCRIBED_AT = '__unsubscribed_at'

    def __init__(self):
        # is better to use composition instead of inheritance
        self.__storage = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__storage.PARTITION_KEY = 'EMAIL_SUBSCRIPTION'
        self.__reflector = Reflector()

    def __get_instance(self, data: dict) -> Subscription:
        subscription_id = Id(data.get('sk'))
        email = Email(data.get('email'))
        user_id = Id(data.get('user_id')) if data.get('user_id') else None
        subscribed_at = datetime.strptime(data.get('subscribed_at'), '%Y-%m-%d %H:%M:%S')
        unsubscribed_at = data.get('unsubscribed_at') or None
        unsubscribed_at = datetime.strptime(unsubscribed_at, '%Y-%m-%d %H:%M:%S') if unsubscribed_at else None

        entity: Subscription = self.__reflector.construct(Subscription, {
            self.__class__.__ENTITY_PROPERTY_SUBSCRIPTION_ID: subscription_id,
            self.__class__.__ENTITY_PROPERTY_EMAIL: email,
            self.__class__.__ENTITY_PROPERTY_USER_ID: user_id,
            self.__class__.__ENTITY_PROPERTY_SUBSCRIBED_AT: subscribed_at,
            self.__class__.__ENTITY_PROPERTY_UNSUBSCRIBED_AT: unsubscribed_at,
        })
        return entity

    def get_by_id(self, subscription_id: Id) -> Optional[Subscription]:
        if not isinstance(subscription_id, Id):
            raise ArgumentTypeException(self.get_by_id, 'subscription_id', subscription_id)

        data = self.__storage.find_item(subscription_id.value)
        return self.__get_instance(data) if data else None

    def get_by_email(self, email: Email) -> Optional[Subscription]:
        if not isinstance(email, Email):
            raise ArgumentTypeException(self.get_by_email, 'email', email)

        data = (self.__storage.find_by_attribute('email', email.value) or [None])[0] or None
        return self.__get_instance(data) if data else None

    def save(self, entity: Subscription) -> None:
        if not isinstance(entity, Subscription):
            raise ArgumentTypeException(self.save, 'entity', entity)

        data = self.__reflector.extract(entity, (
            self.__class__.__ENTITY_PROPERTY_SUBSCRIPTION_ID,
            self.__class__.__ENTITY_PROPERTY_EMAIL,
            self.__class__.__ENTITY_PROPERTY_USER_ID,
            self.__class__.__ENTITY_PROPERTY_SUBSCRIBED_AT,
            self.__class__.__ENTITY_PROPERTY_UNSUBSCRIBED_AT,
        ))

        subscription_id: Id = data[self.__class__.__ENTITY_PROPERTY_SUBSCRIPTION_ID]
        email: Email = data[self.__class__.__ENTITY_PROPERTY_EMAIL]
        user_id: Optional[Id] = data[self.__class__.__ENTITY_PROPERTY_USER_ID]
        subscribed_at: datetime = data[self.__class__.__ENTITY_PROPERTY_SUBSCRIBED_AT]
        unsubscribed_at: Optional[datetime] = data[self.__class__.__ENTITY_PROPERTY_UNSUBSCRIBED_AT]

        self.__storage.put_item(subscription_id.value, {
            'email': email.value,
            'user_id': user_id.value if user_id else None,
            'subscribed_at': subscribed_at.strftime('%Y-%m-%d %H:%M:%S'),
            'unsubscribed_at': unsubscribed_at.strftime('%Y-%m-%d %H:%M:%S') if unsubscribed_at else None
        })


# ----------------------------------------------------------------------------------------------------------------------


class SubscriptionService:
    def __init__(self, storage: SubscriptionStorage, sqs_sender: SqsSenderInterface):
        if not isinstance(storage, SubscriptionStorage):
            raise ArgumentTypeException(self.__init__, 'storage', storage)

        if not isinstance(sqs_sender, SqsSenderInterface):
            raise ArgumentTypeException(self.__init__, 'sqs_sender', sqs_sender)

        self.__storage = storage
        self.__sqs_sender = sqs_sender

    def subscribe(self, email: Email, user_id: Optional[Id]) -> None:
        if not isinstance(email, Email):
            raise ArgumentTypeException(self.subscribe, 'email', email)
        if user_id and not isinstance(user_id, Id):
            raise ArgumentTypeException(self.subscribe, 'user_id', user_id)

        subscription = self.__storage.get_by_email(email)
        if subscription:
            # restore
            subscription.mark_subscribed()
        else:
            # create
            subscription = Subscription(Id(str(uuid.uuid4())), email)
            if user_id:
                subscription.assign_to_user(user_id)

        self.__storage.save(subscription)

        sqs_event = SubscriptionEventSubscribed(subscription)
        self.__sqs_sender.send(sqs_event)

    def unsubscribe(self, subscription_id: Id) -> None:
        if not isinstance(subscription_id, Id):
            raise ArgumentTypeException(self.unsubscribe, 'subscription_id', subscription_id)

        subscription = self.__storage.get_by_id(subscription_id)
        if not subscription:
            raise ApplicationLogicException('Subscription #{} does not exist!'.format(subscription_id))

        subscription.mark_unsubscribe()

        self.__storage.save(subscription)

        sqs_event = SubscriptionEventUnsubscribed(subscription)
        self.__sqs_sender.send(sqs_event)


# ----------------------------------------------------------------------------------------------------------------------


class SubscriptionEventSubscribed(SqsSenderEventInterface):
    @classmethod
    def _get_event_type(cls) -> str:
        return 'subscription_subscribed'

    def __init__(self, subscription: Subscription):
        if not isinstance(subscription, Subscription):
            raise ArgumentTypeException(self.__init__, 'subscription', subscription)

        self.__subscription = subscription

    @property
    def event_data(self) -> dict:
        return {
            'email': self.__subscription.email.value,
            'subscription_id': self.__subscription.subscription_id.value,
            'subscribed_at': self.__subscription.subscribed_at.strftime('%Y-%m-%d %H:%M:%S')
        }


class SubscriptionEventUnsubscribed(SqsSenderEventInterface):
    @classmethod
    def _get_event_type(cls) -> str:
        return 'subscription_unsubscribed'

    def __init__(self, subscription: Subscription):
        if not isinstance(subscription, Subscription):
            raise ArgumentTypeException(self.__init__, 'subscription', subscription)

        self.__subscription = subscription

    @property
    def event_data(self) -> dict:
        return {
            'email': self.__subscription.email.value,
            'unsubscribed_at': self.__subscription.unsubscribed_at.strftime('%Y-%m-%d %H:%M:%S')
        }


# ----------------------------------------------------------------------------------------------------------------------

