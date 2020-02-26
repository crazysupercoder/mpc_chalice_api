import json
from typing import Tuple
from warnings import warn
from boto3 import client
from ...settings import settings


class DataLakeBase(object):
    DELIVERY_STREAM_NAME = settings.DATALAKE_USERTRACK_DELIVERY_STREAM_NAME

    def __init__(self):
        self.__client = client(
            'firehose',
            aws_access_key_id=settings.DATALAKE_AWS_ACCOUNT_ACCESS_KEY_ID,
            aws_secret_access_key=settings.DATALAKE_AWS_ACCOUNT_SECRET_KEY_ID)
        if not self.DELIVERY_STREAM_NAME:
            raise 'Blank delivery stream found.'

    def convert_to_byte(
            self,
            item: dict,
            keep_json: bool = True):
        if keep_json:
            return json.dumps(item) + "\n"
        else:
            result = ""
            for key, value in item.items():
                if isinstance(value, dict):
                    value = json.dumps(value)
                result += ",\"%s\"" % value
            return result + "\n"

    @property
    def client(self):
        return self.__client

    def put_record(self, item) -> Tuple[bool, str]:
        try:
            response = self.client.put_record(
                DeliveryStreamName=self.DELIVERY_STREAM_NAME,
                Record={
                    'Data': self.convert_to_byte(item),
                }
            )
            return True, None
        except Exception as e:
            if settings.DEBUG:
                raise e
            else:
                warn(e)
                return False, str(e)

    def put_record_batch(self, items: list) -> Tuple[bool, str]:
        try:
            response = self.client.put_record_batch(
                DeliveryStreamName=self.DELIVERY_STREAM_NAME,
                Records=[{'Data': self.convert_to_byte(item)} for item in items],
            )
            return True, None
        except Exception as e:
            if settings.DEBUG:
                raise e
            else:
                warn(e)
                return False, str(e)
