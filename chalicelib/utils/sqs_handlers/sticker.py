from .base import *
from chalicelib.libs.models.mpc.Cms.Stickers import *


class StickerSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__stickers_model = StickerModel()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == 'product_sticker':
            self.__handle_save(sqs_message)
        elif message_type == 'product_sticker_delete':
            self.__handle_delete(sqs_message)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

    def __handle_save(self, sqs_message: SqsMessage) -> None:
        sticker = StickerEntity(
            int(sqs_message.message_data.get('id')),
            str(sqs_message.message_data.get('name')),
            str(sqs_message.message_data.get('image', None))
        )
        self.__stickers_model.save(sticker)

    def __handle_delete(self, sqs_message: SqsMessage) -> None:
        sticker_id = int(sqs_message.message_data.get('id'))
        self.__stickers_model.delete(sticker_id)

