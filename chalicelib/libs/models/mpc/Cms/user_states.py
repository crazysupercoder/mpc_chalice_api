"""We are going to move all the features of profile here.
So we will use a single partition key for profile.
But let's use a temp name for now.
"""
from typing import List, Tuple, Union
from warnings import warn
import boto3
from boto3.dynamodb.conditions import Key, Attr
from chalicelib.settings import settings
from chalicelib.libs.core.datetime import (
    get_mpc_datetime_now, datetime, timedelta, DATETIME_FORMAT)
from ..base import DynamoModel


class CustomerStateEntry(object):
    email: str = None
    personalized_at: str = None
    personalize_in_progress: bool = False
    clicked_at: str = None
    personalize_in_progress: bool = False

    def __init__(
            self,
            email: str = None,
            personalized_at: str = None,
            personalize_in_progress: bool = False,
            clicked_at: str = None,
            **kwargs):
        self.personalized_at = personalized_at
        self.clicked_at = clicked_at

    @property
    def is_personalized(self) -> bool:
        return True if self.personalized_at else False



class CustomerStateModel(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY: str = 'PROFILE'
    __state: CustomerStateEntry = None

    def __init__(self, customer_id: str, email: str = None):
        super(CustomerStateModel, self).__init__(self.TABLE_NAME)
        self.__customer_id = customer_id
        self.__email = email

    @staticmethod
    def __wrap_value__(value) -> dict:
        if isinstance(value, str):
            return {'S': value}
        elif isinstance(value, bool):
            return {'B': value}
        else:
            return {'Value': value}

    @property
    def customer_id(self) -> str:
        return self.__customer_id

    @property
    def state(self) -> CustomerStateEntry:
        if not isinstance(self.__state, CustomerStateEntry):
            item: dict = self.table.get_item(
                Key={
                    'pk': self.PARTITION_KEY,
                    'sk': self.customer_id
                }).get('Item', {})
            self.__state = CustomerStateEntry(**item)
        if self.__email and self.__state.email != self.__email:
            self.set_attribute('email', self.__email)
        return self.__state

    @property
    def personalized_at(self) -> str:
        return self.state.personalized_at

    @personalized_at.setter
    def personalized_at(self, value: Union[str, datetime]):
        if isinstance(value, datetime):
            value = value.strftime(DATETIME_FORMAT)
        elif not isinstance(value, str):
            raise Exception("Unknown format - %s" % type(value))
        status, msg = self.set_attribute('personalized_at', value)
        if status:
            self.state.personalized_at = value
        else:
            warn(msg)

    @property
    def personalize_in_progress(self) -> bool:
        return self.state.personalize_in_progress

    @personalize_in_progress.setter
    def personalize_in_progress(self, value: bool):
        if self.personalize_in_progress and not value:
            # change of personalize_in_progress from False to True means complete
            status, msg = self.set_attributes(
                personalize_in_progress=value,
                personalized_at=get_mpc_datetime_now().strftime(DATETIME_FORMAT))
        else:
            status, msg = self.set_attribute('personalize_in_progress', value)

        if status:
            self.state.personalize_in_progress = value
        else:
            warn(msg)

    @property
    def clicked_at(self) -> str:
        return self.state.clicked_at

    @clicked_at.setter
    def clicked_at(self, value: Union[str, datetime]):
        if isinstance(value, datetime):
            value = value.strftime(DATETIME_FORMAT)
        elif not isinstance(value, str):
            raise Exception("Unknown format - %s" % type(value))
        status, msg = self.set_attribute('clicked_at', value)
        if status:
            self.state.clicked_at = value
        else:
            warn(msg)

    def clicked_now(self):
        self.clicked_at = get_mpc_datetime_now()

    def set_attribute(self, attr_name: str, value) -> Tuple[bool, str]:
        return self.set_attributes(**{attr_name: value})

    def set_attributes(self, **kwargs) -> Tuple[bool, str]:
        filtered = dict()
        for key, value in kwargs.items():
            if not hasattr(CustomerStateEntry, key):
                warn("Unknown attr - %s found." % key)
                continue
            filtered.update({
                key: {'Value': value}  # self.__class__.__wrap_value__(value)}
            })
        try:
            self.table.update_item(Key={
                'pk': self.PARTITION_KEY,
                'sk': self.customer_id
            }, AttributeUpdates=filtered)
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_customers_to_recalculate_scores(cls):
        dynamodb = boto3.resource('dynamodb', region_name=cls.AWS_REGION)
        table = dynamodb.Table(cls.TABLE_NAME)
        from_date = (
            get_mpc_datetime_now() - timedelta(
                minutes=settings.SCORE_CALCULATE_INTERVAL)).strftime(DATETIME_FORMAT)
        response = table.query(
            KeyConditionExpression=Key('pk').eq('PROFILE'),
            FilterExpression=Attr('personalized_at').lt(from_date) &
            Attr('clicked_at').gt(from_date) &
            Attr('personalize_in_progress').eq(False)
        )
        records = [item for item in response.get('Items', []) if item.get('email')]
        return [item['email'] for item in sorted(records,
            key=lambda record: record.get('personalized_at') or '')[:settings.CALCULATE_SCORE_BATCH_SIZE]]
