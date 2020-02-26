from typing import Tuple
from datetime import datetime
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.models.mpc.base import DynamoModel


# ----------------------------------------------------------------------------------------------------------------------


class Message(object):
    def __init__(self, message_id: str, customer_email: str, title: str, text: str):
        params_map = {
            'message_id': message_id,
            'customer_email': customer_email,
            'title': title,
            'text': text
        }
        for param_name in params_map:
            param_value = params_map[param_name]
            if not isinstance(param_value, str):
                raise ArgumentTypeException(self.__init__, param_name, param_value)
            elif not str(param_value).strip():
                raise ArgumentCannotBeEmptyException(self.__init__, param_name)

        self.__id = message_id
        self.__customer_email = customer_email
        self.__title = title
        self.__text = text
        self.__created_at = datetime.now()

    @property
    def message_id(self) -> str:
        return self.__id

    @property
    def customer_email(self) -> str:
        return self.__customer_email

    @property
    def title(self) -> str:
        return self.__title

    @property
    def text(self) -> str:
        return self.__text

    @property
    def created_at(self) -> datetime:
        return self.__created_at


# ----------------------------------------------------------------------------------------------------------------------


class MessageStorageInterface(object):
    def save(self, message: Message) -> None:
        raise NotImplementedError()

    def remove(self, message_id: str) -> None:
        raise NotImplementedError()

    def get_all_for_customer(self, customer_email: str) -> Tuple[Message]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class _MessageStorageDynamoDb(MessageStorageInterface):
    __ENTITY_PROPERTY_MESSAGE_ID = '__id'
    __ENTITY_PROPERTY_CUSTOMER_EMAIL = '__customer_email'
    __ENTITY_PROPERTY_TITLE = '__title'
    __ENTITY_PROPERTY_TEXT = '__text'
    __ENTITY_PROPERTY_CREATED_AT = '__created_at'

    def __init__(self):
        # is better to use composition instead of inheritance
        self.__storage = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__storage.PARTITION_KEY = 'NOTIFICATION_SIMPLE'
        self.__reflector = Reflector()

    def save(self, entity: Message) -> None:
        if not isinstance(entity, Message):
            raise ArgumentTypeException(self.save, 'entity', entity)

        data = self.__reflector.extract(entity, (
            self.__class__.__ENTITY_PROPERTY_MESSAGE_ID,
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_EMAIL,
            self.__class__.__ENTITY_PROPERTY_TITLE,
            self.__class__.__ENTITY_PROPERTY_TEXT,
            self.__class__.__ENTITY_PROPERTY_CREATED_AT,
        ))

        self.__storage.put_item(data[self.__class__.__ENTITY_PROPERTY_MESSAGE_ID], {
            'customer_email': data[self.__class__.__ENTITY_PROPERTY_CUSTOMER_EMAIL],
            'title': data[self.__class__.__ENTITY_PROPERTY_TITLE],
            'text': data[self.__class__.__ENTITY_PROPERTY_TEXT],
            'created_at': data[self.__class__.__ENTITY_PROPERTY_CREATED_AT].strftime('%Y-%m-%d %H:%M:%S')
        })

    def remove(self, message_id: str) -> None:
        if not isinstance(message_id, str):
            raise ArgumentTypeException(self.remove, 'message_id', message_id)
        elif not str(message_id).strip():
            raise ArgumentCannotBeEmptyException(self.remove, 'message_id')

        self.__storage.delete_item(message_id)

    def get_all_for_customer(self, customer_email: str) -> Tuple[Message]:
        if not isinstance(customer_email, str):
            raise ArgumentTypeException(self.get_all_for_customer, 'customer_email', customer_email)
        elif not str(customer_email).strip():
            raise ArgumentCannotBeEmptyException(self.get_all_for_customer, 'customer_email')

        rows = self.__storage.filter_by_field_value('customer_email', customer_email)
        return tuple([self.__get_instance(row) for row in rows])

    def __get_instance(self, data: dict) -> Message:
        entity: Message = self.__reflector.construct(Message, {
            self.__class__.__ENTITY_PROPERTY_MESSAGE_ID: data.get('sk'),
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_EMAIL: data.get('customer_email'),
            self.__class__.__ENTITY_PROPERTY_TITLE: data.get('title'),
            self.__class__.__ENTITY_PROPERTY_TEXT: data.get('text'),
            self.__class__.__ENTITY_PROPERTY_CREATED_AT: datetime.strptime(data.get('created_at'), '%Y-%m-%d %H:%M:%S')
        })
        return entity


# ----------------------------------------------------------------------------------------------------------------------


class MessageStorageImplementation(MessageStorageInterface):
    def __init__(self):
        self.__implementation = _MessageStorageDynamoDb()

    def save(self, message: Message) -> None:
        self.__implementation.save(message)

    def remove(self, message_id: str) -> None:
        self.__implementation.remove(message_id)

    def get_all_for_customer(self, customer_email: str) -> Tuple[Message]:
        return self.__implementation.get_all_for_customer(customer_email)


# ----------------------------------------------------------------------------------------------------------------------

