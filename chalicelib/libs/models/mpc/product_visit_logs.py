import boto3
from datetime import datetime, timedelta
from collections import deque
from boto3.dynamodb.conditions import Key, Attr
from ....settings import settings
from .base import DynamoModel


class ProductVisitLog(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PROFILE#%s'
    GUEST_USER_PARTITION_KEY = 'GUEST#%s'
    SORT_KEY = 'PRODUCT_VISIT_LOG'
    __session_id = None
    __customer_id = None

    def __init__(self, session_id, customer_id=None, **kwargs):
        super(ProductVisitLog, self).__init__(self.TABLE_NAME)
        if customer_id is not None:
            self.customer_id = customer_id
        if session_id is None:
            raise Exception('session_id is required.')
        self.session_id = session_id

    def now(self, fmt="%Y-%m-%d %H:%M:%S"):
        return datetime.now().strftime(fmt)

    def get_partition_key(self):
        if self.customer_id is None:
            return self.GUEST_USER_PARTITION_KEY % self.session_id
        else:
            return self.PARTITION_KEY % self.customer_id

    @property
    def session_id(self):
        return self.__session_id

    @session_id.setter
    def session_id(self, value):
        self.__session_id = value

    @property
    def customer_id(self):
        return self.__customer_id

    @customer_id.setter
    def customer_id(self, value):
        self.__customer_id = value

    def get_item(self, **kwargs):
        item = self.table.get_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': self.SORT_KEY
            }
        )
        return item.get('Item', )

    def get_logs(self, omit=None):
        item = self.get_item()
        if item is None:
            return []
        else:
            logs = item.get('logs', [])
            from_date = (datetime.now() - timedelta(days=settings.PRODUCT_VISIT_LOG_THRESHOLD)).strftime("%Y-%m-%d %H:%M:%S")
            logs = [log for log in logs if log['visited_at'] > from_date]
            if omit is None:
                return logs
            else:
                return [log for log in logs if log.get('id') != omit]

    def insert(self, product_item, **kwargs):
        item = self.get_item()

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        import json
        from decimal import Decimal
        product_item = json.loads(json.dumps(product_item), parse_float=Decimal)

        product_item['visited_at'] = self.now()
        if item is None:
            response = self.table.put_item(
                Item={
                    'pk': self.get_partition_key(),
                    'sk': self.SORT_KEY,
                    'logs': [product_item]
                })
        else:
            logs = item.get('logs', [])
            for idx, value in enumerate(logs):
                value_id = value.get('id', value.get('portal_config_id'))
                product_item_id = product_item.get('id', product_item.get('portal_config_id'))
                if value_id == product_item_id:
                    del logs[idx]
            logs = deque(logs, settings.PRODUCT_VISIT_LOG_MAX)
            logs.appendleft(product_item)
            response = self.table.update_item(
                Key={
                    'pk': self.get_partition_key(),
                    'sk': self.SORT_KEY
                },
                UpdateExpression="set logs=:logs",
                ExpressionAttributeValues={
                    ':logs': list(logs)
                }
            )
        return response
