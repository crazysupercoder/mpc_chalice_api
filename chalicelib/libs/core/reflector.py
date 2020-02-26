import inspect
from typing import Union


class Reflector(object):
    def __get_full_property_name(self, cls, property_name: str) -> str:
        # strange name
        if property_name == '_':
            return property_name

        # public self.a
        if property_name[0] != '_':
            return property_name

        # protected self._b
        if property_name[1] != '_':
            return property_name

        # private self.__c
        class_name = cls.__name__
        for i in range(0, len(class_name)):
            if class_name[i] != '_':
                class_name = class_name[i:]
                break

        return '_' + class_name + property_name

    def __get_entity_property(self, entity, property_name: str):
        full_property_name = self.__get_full_property_name(type(entity), property_name)
        return getattr(entity, full_property_name)

    def __set_entity_property(self, entity, property_name: str, value) -> None:
        full_property_name = self.__get_full_property_name(type(entity), property_name)
        setattr(entity, full_property_name, value)

    def extract(self, entity, properties_list: Union[tuple, list]) -> dict:
        if not entity:
            raise TypeError('{} parameter "{}" must be {}, but {} is given!'.format(
                self.extract.__qualname__,
                'entity',
                'existed object',
                type(entity).__name__
            ))

        if not isinstance(properties_list, (tuple, list, set)):
            raise TypeError('{} parameter "{}" must be {}, but {} is given!'.format(
                self.extract.__qualname__,
                'properties_map',
                tuple.__name__ + ' or ' + list.__name__ + ' or ' + set.__name__,
                type(properties_list).__name__
            ))

        result = {}
        for property_name in properties_list:
            result[property_name] = self.__get_entity_property(entity, property_name)
        return result

    def construct(self, cls, properties_map: dict):
        if not inspect.isclass(cls):
            raise TypeError('{} parameter "{}" must be {}, but {} is given!'.format(
                self.construct.__qualname__,
                'cls',
                'class',
                type(cls).__name__
            ))

        if not isinstance(properties_map, dict):
            raise TypeError('{} parameter "{}" must be {}, but {} is given!'.format(
                self.construct.__qualname__,
                'properties_map',
                dict.__name__,
                type(properties_map).__name__
            ))

        entity = object.__new__(cls)
        for (property_name, property_value) in tuple(properties_map.items()):
            self.__set_entity_property(entity, property_name, property_value)
        return entity

