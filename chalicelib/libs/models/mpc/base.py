import uuid
import json
import boto3
from typing import Tuple, Optional, List
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from chalicelib.settings import settings


class Base:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        
    def get_table(self):
        return self.__tbl

    @staticmethod
    def dump_json(data):
        return json.dumps(data)

    def json_from_string(self, data):
        return json.loads(data)

    @staticmethod
    def new_guid():
        return uuid.uuid4().__str__()

    @staticmethod
    def timestamp():
        return datetime.utcnow().__str__()


class DynamoModel(object):
    TABLE_NAME = None
    AWS_REGION = settings.AWS_DYNAMODB_DEFAULT_REGION
    PARTITION_KEY = ''

    def __init__(self, table_name):
        if table_name is None:
            raise NotImplementedError('table_name is required.')
        self.TABLE_NAME = table_name

    @property
    def table(self):
        dynamodb = boto3.resource('dynamodb', region_name=self.AWS_REGION)
        return dynamodb.Table(self.TABLE_NAME)

    @table.setter
    def table(self, value):
        self.TABLE_NAME = value

    def get_partition_key(self):
        return self.PARTITION_KEY

    def insert_data(self, records, bulk_flag=True):
        if bulk_flag:
            with self.table.batch_writer() as writer:
                for item in records:
                    writer.put_item(Item=item)
        else:
            raise NotImplementedError()

    def insert_item(self, sk, item):
        item.update({
                'pk': self.get_partition_key(),
                'sk': str(sk)
            })
        try:
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            return False

    def filter_by_field_in_array(self, field_name, values=[], value_type=str, lower=True):
        sk_condition_attributes = []
        sk_condition_attr_values = {}
        # for idx, value in enumerate(values):
        #     attr_name = ":%s%d" % (field_name, idx)
        #     sk_condition_attributes.append(attr_name)
        #     if value_type == str:
        #         sk_condition_attr_values[attr_name] = value_type(value).lower() if lower else value_type(value)
        #     else:
        #         sk_condition_attr_values[attr_name] = value_type(value)

        # filter_expression = "%s IN (%s)" % (field_name, ",".join(sk_condition_attributes))

        if value_type == str:
            values = [value_type(value).lower() if lower else value_type(value) for value in values]
        else:
            values = [value_type(value) for value in values]

        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
            # FilterExpression=filter_expression,
            # ExpressionAttributeValues=sk_condition_attr_values
            FilterExpression=Attr(field_name).is_in(values)
        )
        return response

    def convert_item(self, item):
        item.update({
            'image': {
                'src': item.get('image'),
                'title': item.get('product_type_name')
            }
        })
        return item

    def filter_by_field_value(self, field_name, value) -> List[dict]:
        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
            FilterExpression=Attr(field_name).eq(value)
        )
        return [self.convert_item(item) for item in response['Items']]

    def find_by_attribute(self, attribute_name: str, value) -> Tuple[dict]:
        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
            FilterExpression=Attr(attribute_name).eq(value)
        )
        return tuple(response['Items'])

    def find_all(self):
        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key())
        )
        return tuple(response['Items'])

    def get_item(self, sk):
        response = self.table.get_item(Key={
            'pk': self.get_partition_key(),
            'sk': str(sk)
        })
        return response

    def find_item(self, item_id: str) -> Optional[dict]:
        """ the same as get_item(), but returns data only """
        response = self.get_item(item_id)
        return response.get('Item') if response else None

    def put_item(self, item_id: str, item_data: dict) -> None:
        """ the same as insert_data(), but throws exceptions """
        data = {}
        for k in tuple(item_data.keys()):
            data[k] = item_data.get(k, None)

        data['pk'] = self.get_partition_key()
        data['sk'] = item_id

        self.table.put_item(Item=data)

    def delete_item(self, sk):
        response = self.table.delete_item(Key={
            'pk': self.get_partition_key(),
            'sk': str(sk)
        })
        return response
