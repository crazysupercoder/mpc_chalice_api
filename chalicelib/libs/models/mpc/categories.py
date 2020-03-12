from typing import List, Tuple, Optional
from chalicelib.extensions import *
from boto3.dynamodb.conditions import Key
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.settings import settings
from chalicelib.libs.models.ml.products import Product


class CategoryGender(object):
    def __init__(self, id: int, name: str):
        if not isinstance(id, int):
            raise ArgumentTypeException(self.__init__, 'id', id)
        elif not id:
            raise ArgumentCannotBeEmptyException(self.__init__, 'id')

        if not isinstance(name, str):
            raise ArgumentTypeException(self.__init__, 'name', name)
        elif not str(name).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'name')

        self.__id = id
        self.__name = name

    @property
    def id(self) -> int:
        return self.__id

    @property
    def name(self) -> str:
        return self.__name


class CategorySize(object):
    def __init__(self, name: str):
        if not isinstance(name, str):
            raise ArgumentTypeException(self.__init__, 'name', name)
        elif not str(name).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'name')

        self.__name = str(name).strip()

    @property
    def name(self) -> str:
        return self.__name


class CategoryProductType(object):
    def __init__(self, id: int, name: str, sizes: Tuple[CategorySize]):
        if not isinstance(id, int):
            raise ArgumentTypeException(self.__init__, 'id', id)
        elif not id:
            raise ArgumentCannotBeEmptyException(self.__init__, 'id')

        if not isinstance(name, str):
            raise ArgumentTypeException(self.__init__, 'name', name)
        elif not str(name).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'name')

        if not isinstance(sizes, Tuple) or sum([not isinstance(size, CategorySize) for size in sizes]) > 0:
            raise ArgumentTypeException(self.__init__, 'sizes', sizes)

        self.__id = id
        self.__name = name
        self.__sizes = sizes

    @property
    def id(self) -> int:
        return self.__id

    @property
    def name(self) -> str:
        return self.__name

    @property
    def sizes(self) -> Tuple[CategorySize]:
        return self.__sizes


class CategorySubType(CategoryProductType):
    def __init__(self, id: int, name: str, sizes: Tuple[CategorySize], image_url: str):
        super().__init__(id, name, sizes)

        if not isinstance(image_url, str):
            raise ArgumentTypeException(self.__init__, 'image_url', image_url)
        elif not str(image_url).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'image_url')

        self.__image_url = image_url

    @property
    def image(self) -> str:
        return self.__image_url


class CategoryEntry:
    def __init__(
        self,
        id: int,
        name: str,
        image: str,
        gender: CategoryGender,
        product_type: CategoryProductType,
        subtypes: Tuple[CategorySubType]
    ):
        if not isinstance(gender, CategoryGender):
            raise ArgumentTypeException(self.__init__, 'gender', gender)

        if not isinstance(product_type, CategoryProductType):
            raise ArgumentTypeException(self.__init__, 'product_type', product_type)

        if not isinstance(subtypes, tuple) or sum([not isinstance(subtype, CategorySubType) for subtype in subtypes]) > 0:
            raise ArgumentTypeException(self.__init__, 'subtypes', subtypes)

        self.__id = id
        self.__name = name
        self.__image = image
        self.__gender = gender
        self.__product_type = product_type
        self.__subtypes = subtypes

    @property
    def id(self) -> int:
        return self.__id

    @property
    def name(self) -> str:
        return self.__name

    @property
    def image(self) -> str:
        return self.__image

    @property
    def gender_id(self) -> int:
        return self.__gender.id

    @property
    def gender_name(self) -> str:
        return self.__gender.name

    @property
    def product_type_id(self) -> int:
        return self.__product_type.id

    @property
    def product_type_name(self) -> str:
        return self.__product_type.name

    @property
    def subtypes(self) -> Tuple[CategorySubType]:
        return self.__subtypes

    @property
    def sizes(self) -> Tuple[CategorySize]:
        sizes = {}

        for size in self.__product_type.sizes:
            sizes[size.name] = size

        for subtype in self.__subtypes:
            for size in subtype.sizes:
                sizes[size.name] = size

        sizes = tuple(sizes.values())
        return sizes

    def to_dict(self):
        return {
            "category_id": self.id,
            "name": self.name,
            "image": self.image,
            "gender_id": self.gender_name,
            "gender_name": self.gender_name,
            "product_type_id": self.product_type_id,
            "product_type_name": self.product_type_name,
            "subtypes": [{
                "subtype_id": subtype.id,
                "subtype_name": subtype.name,
                "image_url": subtype.image,
            } for subtype in self.subtypes],
            "sizes": [size.name for size in self.sizes],
        }


# ----------------------------------------------------------------------------------------------------------------------


class Category(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'CATEGORY'

    def __init__(self):
        super(Category, self).__init__(self.TABLE_NAME)

    def __create_entity(self, row: dict) -> CategoryEntry:
        return CategoryEntry(
            int(row.get('category_id')),
            row.get('name'),
            row.get('image'),
            CategoryGender(
                int(row.get('gender_id')),
                row.get('gender_name')
            ),
            CategoryProductType(
                int(row.get('product_type_id')),
                row.get('product_type_name'),
                tuple([CategorySize(size_name) for size_name in row.get('sizes') or []])
            ),
            tuple([
                CategorySubType(
                    int(item.get('subtype_id')),
                    item.get('subtype_name'),
                    tuple([CategorySize(size_name) for size_name in item.get('sizes') or []]),
                    item.get('image_url')
                ) for item in row.get('subtypes')
            ])
        )

    def get_by_gender(
            self, gender: str, **kwargs) -> List[dict]:
        """Get categories for a given gender
        - gender: string, default is unisex
        - customer_id: customer's email address to be used in personalize
        """
        if gender is None or gender.lower() == 'unisex':
            gender = 'ladies'

        categories = self.filter_by_field_value('gender_name', gender.upper())
        return categories

    def get_categories(
            self, exclude: List[str]=[],
            genders: List[str]=None,
            **kwargs) -> List[CategoryEntry]:
        """
        Get categories for a specific customer
        - exclude: 
        """
        categories = self.table.query(KeyConditionExpression=Key('pk').eq(self.get_partition_key())).get('Items', [])
        categories = [self.__create_entity(item) for item in categories if item['category_id'] not in exclude]
        
        if genders is not None and isinstance(genders, list):
            categories = [
                item for item in categories
                if item.gender_name.upper() in [
                    g.upper() for g in genders]]
        return categories

    def get_products_by_id(self, category_id, page=1, size=18):
        category = self.get_item(category_id)
        if category_id is None:
            return []
        product_model = Product()
        return product_model.get_products_by_category(
            category['product_type_name'],
            [item['subtype_name'] for item in category['subtypes']],
            category['gender_name'], page=page, size=size)

    def get_item_v2(self, sk: str) -> Optional[CategoryEntry]:
        row = super(Category, self).get_item(str(sk)).get('Item')
        return self.__create_entity(row) if row else None

    def get_by_ids(self, ids: Tuple[int]) -> Tuple[CategoryEntry]:
        if not isinstance(ids, tuple) or sum([not isinstance(id, int) for id in ids]) > 0:
            raise ArgumentTypeException(self.get_by_ids, 'ids', ids)

        rows = self.table.query(KeyConditionExpression=Key('pk').eq(self.get_partition_key())).get('Items') or []

        result = []
        for row in rows:
            if int(row.get('category_id')) in ids:
                result.append(self.__create_entity(row))

        result = tuple(result)

        return result

