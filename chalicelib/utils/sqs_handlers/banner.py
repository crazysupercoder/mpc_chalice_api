from .base import *
from chalicelib.libs.models.mpc.Cms.Banners import Banners as BannerAdmin


class BannerSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__banners = BannerAdmin()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == 'mpc_banner':
            self.__banners.insert(sqs_message.message_data)
        elif message_type == 'mpc_banner_delete':
            self.__banners.delete(sqs_message.message_data['banner_id'])
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

