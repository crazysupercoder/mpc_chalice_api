from chalicelib.settings import settings
from ..base import Base
from ..product_types import ProductType


class ASSET_TYPE:
    category = 'category'


class Assets(Base):

    def __init__(self):
        Base.__init__(self)
        self.table = self.dynamodb.Table(settings.AWS_DYNAMODB_CMS_TABLE_NAME)

    def insert(self, data: dict, identifier):
        if identifier == ASSET_TYPE.category:
            product_type_model = ProductType()
            category_id = str(data.get('id'))
            data['pk'] = ASSET_TYPE.category.upper()
            data['sk'] = category_id
            data['category_id'] = category_id

            product_type_model.create_or_update(
                data['product_type_id'], 
                {
                    'product_gender': None,
                    'product_gender_id': 0,
                    'product_type_id': int(data['product_type_id']),
                    'product_type_code': data['product_type_name'].lower(),
                    'product_type_name': data['product_type_name'],
                    'parent_id': 0,
                    'image': data['image'],
                    'sizes': data.get('sizes', []) or [],
                }
            )

            for subtype in data['subtypes']:
                product_type_model.create_or_update(
                    subtype['subtype_id'],
                    {
                        'product_gender': data['gender_name'],
                        'product_gender_id': data['gender_id'],
                        'product_type_id': int(subtype['subtype_id']),
                        'product_type_code': subtype['subtype_name'].lower(),
                        'product_type_name': subtype['subtype_name'],
                        'parent_id': int(data['product_type_id']),
                        'image': subtype['image_url'] or 'http://lorempixel.com/100/100/people',
                        'sizes': data.get('sizes', []) or [],
                    })

            for item in [subtype for subtype in data['subtypes'] if not subtype['image_url']]:
                item['image_url'] = 'http://lorempixel.com/100/100/people'
        else:
            raise NotImplementedError('Unknown identifier for assets.')

        key = {'pk': data['pk'], 'sk': data['sk']}
        if self.table.get_item(Key=key).get('Item') is not None:
            self.table.delete_item(Key=key)
        self.table.put_item(Item=data)
