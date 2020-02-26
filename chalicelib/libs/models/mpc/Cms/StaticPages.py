from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.reflector import Reflector
from chalicelib.libs.models.mpc.base import DynamoModel


# ----------------------------------------------------------------------------------------------------------------------


class StaticPage(object):
    def __init__(self, descriptor: str, name: str, content: str):
        parameters_map = {
            'descriptor': descriptor,
            'name': name,
            'content': content
        }
        for (k, v) in tuple(parameters_map.items()):
            if not isinstance(v, str):
                raise ArgumentTypeException(self.__init__, k, v)
            elif not str(v).strip():
                raise ArgumentCannotBeEmptyException(self.__init__, k)

        self.__descriptor = str(descriptor).strip()
        self.__name = str(name).strip()
        self.__content = str(content).strip()

    @property
    def descriptor(self) -> str:
        return self.__descriptor

    @property
    def name(self) -> str:
        return self.__name

    @property
    def content(self) -> str:
        return self.__content


# ----------------------------------------------------------------------------------------------------------------------


class StaticPageStorage(object):
    __ENTITY_PROPERTY_DESCRIPTOR = '__descriptor'
    __ENTITY_PROPERTY_NAME = '__name'
    __ENTITY_PROPERTY_CONTENT = '__content'

    def __init__(self):
        # is better to use composition instead of inheritance
        self.__storage = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__storage.PARTITION_KEY = 'STATIC_PAGE'
        self.__reflector = Reflector()

    def save(self, entity: StaticPage) -> None:
        if not isinstance(entity, StaticPage):
            raise ArgumentTypeException(self.save, 'entity', entity)

        data = self.__reflector.extract(entity, (
            self.__class__.__ENTITY_PROPERTY_DESCRIPTOR,
            self.__class__.__ENTITY_PROPERTY_NAME,
            self.__class__.__ENTITY_PROPERTY_CONTENT,
        ))

        self.__storage.put_item(data[self.__class__.__ENTITY_PROPERTY_DESCRIPTOR], {
            'name': data[self.__class__.__ENTITY_PROPERTY_NAME],
            'content': data[self.__class__.__ENTITY_PROPERTY_CONTENT]
        })

    def __get_instance(self, data: dict) -> StaticPage:
        entity: StaticPage = self.__reflector.construct(StaticPage, {
            self.__class__.__ENTITY_PROPERTY_DESCRIPTOR: data['sk'],
            self.__class__.__ENTITY_PROPERTY_NAME: data['name'],
            self.__class__.__ENTITY_PROPERTY_CONTENT: data['content'],
        })
        return entity

    def get_by_descriptor(self, descriptor: str) -> Optional[StaticPage]:
        if not isinstance(descriptor, str):
            raise ArgumentTypeException(self.get_by_descriptor, 'descriptor', descriptor)
        elif not str(descriptor).strip():
            raise ArgumentCannotBeEmptyException(self.get_by_descriptor, 'descriptor')

        data = self.__storage.find_item(descriptor)
        return self.__get_instance(data) if data else None

    def remove(self, descriptor: str) -> None:
        if not isinstance(descriptor, str):
            raise ArgumentTypeException(self.get_by_descriptor, 'descriptor', descriptor)
        elif not str(descriptor).strip():
            raise ArgumentCannotBeEmptyException(self.get_by_descriptor, 'descriptor')

        self.__storage.delete_item(descriptor)

