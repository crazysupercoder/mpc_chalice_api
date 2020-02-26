from boto3.dynamodb.conditions import Key, Attr
from chalicelib.settings import settings
from .base import DynamoModel


class ProductType(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PRODUCT_TYPE'

    def __init__(self):
        super(ProductType, self).__init__(self.TABLE_NAME)

    # ------------------------------------------------------------------------------------------------------------------

    def filter_by_product_type_ids(self, ids):
        return self.filter_by_field_in_array('product_type_id', ids, value_type=int)

    def filter_by_product_type_name(self, product_types, root_only=True):
        if len(product_types) == 0:
            return []

        sk_condition_attributes = []
        sk_condition_attr_values = {}
        if root_only:
            response = self.table.query(
                KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
                FilterExpression=Attr('parent_id').eq(0) & Attr('product_type_code').is_in(
                    [item.lower() for item in product_types])
            )
        else:
            response = self.table.query(
                KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
                FilterExpression=Attr('product_type_code').is_in(
                    [item.lower() for item in product_types])
            )
        return response['Items']

    def get_root_product_types(self):
        return self.filter_by_field_value('parent_id', 0)

    def get_child_product_types(self, parent_id, check_stock=False, gender=None, **kwargs):
        items = self.filter_by_field_value('parent_id', int(parent_id))
        result = list()
        dictionary = dict()
        for item in items:
            if dictionary.get(item['product_type_name']) is None:
                if gender is not None and gender.lower() != 'unisex' and\
                        item['product_gender'] != gender.upper():
                    continue
                dictionary[item['product_type_name']] = True
                result.append(item)
            else:
                continue
        if check_stock:
            from ..ml.products import Product
            product_model = Product()
            result = product_model.filter_subtypes_in_stock(
                result, gender=gender)
        return result

    def get_family_tree(self, node):
        child_nodes = self.get_child_product_types(node['product_type_id'])
        if len(child_nodes) == 0:
            return node
        children = list()
        for child in child_nodes:
            children.append(self.get_family_tree(child))
        node['children'] = children
        return node

    def find_by_id(self, product_type_id):
        response = self.table.get_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': str(product_type_id)
            }
        )
        if response.get('Item') is None:
            return None
        return self.convert_item(response['Item'])

    def get_root_node(self, product_type_id=None, product_type_name=None, **kwargs):
        if product_type_id is None and product_type_name is None:
            raise Exception('product_type_id or product_type_name is required.')
        elif product_type_id is not None and product_type_name is not None:
            raise Exception('You can not give both of product_type_id and name.')
        
        node = None
        if product_type_id is not None:
            node = self.find_by_id(product_type_id)
        if product_type_name is not None:
            nodes = self.filter_by_product_type_name([product_type_name], root_only=False)
            if len(nodes) > 0:
                node = nodes[0]
        if node is not None and node['parent_id'] > 0:
            parent_id = node['parent_id']
            return self.get_root_node(product_type_id=parent_id)
        return node

    def get_tree(self, start_node_id=0):
        if start_node_id == 0:
            root_nodes = self.get_root_product_types()
        else:
            root_nodes = self.get_child_product_types(start_node_id)
        result = list()
        for node in root_nodes:
            result.append(self.get_family_tree(node))
        return result

    # ------------------------------------------------------------------------------------------------------------------

    def create_or_update(self, sk, item):
        response = self.table.get_item(Key={
            'pk': self.get_partition_key(),
            'sk': str(sk)
        })
        if response.get('Item') is None:
            return self.insert_item(sk, item)
        else:
            return self.update_item(sk, item)

    def update_item(self, sk, item):
        return self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': str(sk)
            }, AttributeUpdates=self.__update_attributes(item))

    def batch_update(self, items):
        with self.table.batch_writer() as writer:
            for item in items:
                writer.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': str(item['product_type_id'])
                }, AttributeUpdates=self.__update_attributes(item))

    def __update_attributes(self, item):
        return {
            'product_gender_id': {'Value': item['product_gender_id']},
            'product_gender': {'Value': item['product_gender']},
            'product_type_id': {'Value': item['product_type_id']},
            'product_type_code': {'Value': item['product_type_code'].lower()},
            'product_type_name': {'Value': item['product_type_name']},
            'parent_id': {'Value': item['parent_id']},
            'image': {'Value': item['image']},
            'sizes': {'Value': item['sizes']},
        }
