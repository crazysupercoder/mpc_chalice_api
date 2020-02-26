from .base import *
from chalicelib.libs.models.mpc.brands import Brand


class BrandSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__brands = Brand()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == 'mpc_assets_brands':
            self.__brands.insert(sqs_message.message_data)
        elif message_type == 'mpc_assets_brands_delete':
            self.__brands.delete(sqs_message.message_data)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

