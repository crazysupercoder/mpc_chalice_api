from .base import *
from chalicelib.libs.models.mpc.Cms.Assets import Assets, ASSET_TYPE
from chalicelib.libs.models.mpc.product_types import ProductType


class ProductTypeSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__assets = Assets()
        self.__product_types = ProductType()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == 'mpc_assets_product_type':
            self.__assets.insert(sqs_message.message_data, ASSET_TYPE.category)
        elif message_type == 'mpc_assets_product_type_delete':
            self.__product_types.delete_item(sqs_message.message_data['id'])
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

