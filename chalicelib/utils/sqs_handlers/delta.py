from .base import *
from chalicelib.libs.models.ml.delta_cache import DeltaCache
from chalicelib.libs.models.mpc.Cms.meta import Meta
from chalicelib.constants.sqs import DELTA_CACHE_MESSAGE_TYPES


class DeltaCacheSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__cache = DeltaCache()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == DELTA_CACHE_MESSAGE_TYPES.CACHE_UPDATE:
            self.__cache.update(**sqs_message.message_data)
        elif message_type == DELTA_CACHE_MESSAGE_TYPES.SECRET_KEY:
            meta = Meta()
            meta.secret_key = sqs_message.message_data.get('value')
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)
