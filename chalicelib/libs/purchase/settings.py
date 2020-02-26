import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from chalicelib.settings import settings
from chalicelib.utils.sqs_handlers.base import SqsMessage, SqsHandlerInterface


# ----------------------------------------------------------------------------------------------------------------------


class PurchaseSettings(object):
    __PARTITION_KEY = 'PURCHASE_SETTINGS'
    __VAT_SORT_KEY = 'VAT'

    __DELIVERY_FEE_PARTITION_KEY = 'DYNAMIC_DELIVERY_FEES'
    __DELIVERY_FEE_SORT_KEY = 'FEE'

    def __init__(self):
        self.__table = boto3.resource('dynamodb').Table(settings.AWS_DYNAMODB_CMS_TABLE_NAME)

    @property
    def fee(self) -> float:
        response = self.__table.query(
            KeyConditionExpression=
                Key('pk').eq(self.__class__.__DELIVERY_FEE_PARTITION_KEY) &
                Key('sk').eq(self.__class__.__DELIVERY_FEE_SORT_KEY)
        )
        data = (response.get('Items', [None]) or [None])[0]

        return float(data['fee_value']) if data else 0.0

    @fee.setter
    def fee(self, amount: float) -> None:
        data = {
            'pk': self.__class__.__DELIVERY_FEE_PARTITION_KEY,
            'sk': self.__class__.__DELIVERY_FEE_SORT_KEY,
            'fee_value': amount,
            'fee_enabled': 1 if amount > 0 else 0,
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        data = json.loads(json.dumps(data), parse_float=Decimal)

        self.__table.put_item(Item=data)

    @property
    def vat(self) -> float:
        response = self.__table.query(
            KeyConditionExpression=
                Key('pk').eq(self.__class__.__PARTITION_KEY) &
                Key('sk').eq(self.__class__.__VAT_SORT_KEY)
        )
        data = (response.get('Items', [None]) or [None])[0]
        return float(data['percent'])

    @vat.setter
    def vat(self, percent: float) -> None:
        data = {
            'pk': self.__class__.__PARTITION_KEY,
            'sk': self.__class__.__VAT_SORT_KEY,
            'percent': percent,
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        data = json.loads(json.dumps(data), parse_float=Decimal)

        self.__table.put_item(Item=data)


# ----------------------------------------------------------------------------------------------------------------------


class PurchaseSettingsSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__purchase_settings = PurchaseSettings()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        # @todo : make "dynamic_delivery_fees" a parameter!
        # @todo : rename on portal side
        if message_type == 'dynamic_delivery_fees':
            self.__handle_delivery_fee(sqs_message)
        elif message_type == 'parameters':
            self.__handle_parameters(sqs_message)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

    def __handle_delivery_fee(self, sqs_message: SqsMessage) -> None:
        is_enabled = int(sqs_message.message_data['fee_enabled']) > 0
        self.__purchase_settings.fee = float(sqs_message.message_data['fee_value']) if is_enabled else 0.0

    def __handle_parameters(self, sqs_message: SqsMessage) -> None:
        parameter_type = sqs_message.message_data['parameter_type']
        parameter_data = sqs_message.message_data['parameter_data']

        if parameter_type == 'vat':
            self.__purchase_settings.vat = float(parameter_data['value'])
        else:
            raise ValueError('{} does not know, how to process "{}" SQS Message {}'.format(
                self.__class__.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))


# ----------------------------------------------------------------------------------------------------------------------

