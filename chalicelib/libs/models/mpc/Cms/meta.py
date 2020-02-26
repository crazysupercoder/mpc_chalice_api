from typing import Union, Dict
from datetime import datetime
from boto3.dynamodb.conditions import Key
from typing import List, Tuple
from chalicelib.settings import settings
from ..base import DynamoModel, boto3


class Meta(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'MPC_META'
    DELTA_SECRET_KEY_SK = 'DELTA_SECRET_KEY'
    __scoring_weights_attr_name = 'weights'

    def __init__(self, ):
        super(Meta, self).__init__(self.TABLE_NAME)

    @property
    def secret_key(self) -> str:
        item = self.get_item(self.DELTA_SECRET_KEY_SK).get('Item', {})
        return item.get('secret_key')

    @secret_key.setter
    def secret_key(self, key: str):
        try:
            response = self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': self.DELTA_SECRET_KEY_SK,
            }, AttributeUpdates={
                'secret_key': {'Value': key},
                'updated_at': {'Value': datetime.now().strftime("%Y/%m/%d %H:%M")}
            })
        except Exception as e:
            print(str(e))

    def check_secret_key(self, key: str) -> bool:
        return self.secret_key == key
