import boto3
import json
from typing import List
from ....settings import settings
from .base import DynamoModel


class BrandEntity:
    pk = 'BRAND'
    __id = None
    __name = None
    __code = None
    __image = None
    __description = None

    def __init__(
            self,
            id: str,
            name: str,
            image: str,
            description: str = "",
            **kwargs):
        self.id = id
        self.name = name
        self.code = name.lower()
        self.image = image
        self.description = description

    @property
    def id(self) -> str:
        return self.__id
    
    @id.setter
    def id(self, value: str):
        self.__id = value

    @property
    def name(self) -> str:
        return self.__name
    
    @name.setter
    def name(self, value: str):
        self.__name = value

    @property
    def code(self) -> str:
        return self.__code
    
    @code.setter
    def code(self, value: str):
        self.__code = value.lower()

    @property
    def image(self) -> str:
        return self.__image
    
    @image.setter
    def image(self, value: str):
        self.__image = value

    @property
    def description(self) -> str:
        return self.__description
    
    @description.setter
    def description(self, value: str):
        self.__description = value

    def __str__(self):
        return "<BrandEntity: %s>" % self.name

    def to_dict(self):
        return {
            "id": self.id,
            "brand_name": self.name,
            "brand_code": self.code,
            "logo_url": self.image,
            "description": self.description,
        }


class Brand(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'BRAND'

    def __init__(self):
        super(Brand, self).__init__(self.TABLE_NAME)

    def get_all_brands(self):
        items = self.find_all()
        return items
        # return [
        #     BrandEntity(
        #         item['id'],
        #         item['brand'],
        #         item.get('brand_image_url'),
        #         description=item.get('description')
        #     ) for item in items
        # ]

    def get_brands_with_names(self, names: List[str]) -> List[BrandEntity]:
        items = self.filter_by_field_in_array('brand_name', names)
        return [
            BrandEntity(
                item['id'],
                item['brand'],
                item.get('brand_image_url'),
                description=item.get('description')
            ) for item in items
        ]

    def insert(self, data):
        if type(data) != dict:
            data = json.loads(data)

        entry = {
            'pk': 'BRAND',
            'sk': str(data['id']),
            'brand': str(data['brand_name']),
            'brand_code': str(data['brand_code']),
            'brand_name': str(data['brand_name']).lower(),
            'description': str(data['description']),
            'logo_url': str(data['logo_url'])
        }

        if not entry['logo_url']:
            del entry['logo_url']

        if not entry['description']:
            del entry['description']

        return self.table.put_item(Item=entry)

    def delete(self, data):
        if type(data) != dict:
            data = json.loads(data)
        key = {'sk': int(data['id'])}
        return self.table.delete_item(Key=key)

    def filter_by_brand_names(self, names):
        return self.filter_by_field_in_array('brand_name', names)
