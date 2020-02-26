from .base import *
from chalicelib.constants.sqs import SCORED_PRODUCT_MESSAGE_TYPE
from chalicelib.libs.models.mpc.Cms.meta import Meta


class ScoredProductSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__cache = ScoredProduct()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER:
            self.__cache.calculate_scores(**sqs_message.message_data)
        else:
            raise ValueError(
                'SQS Message type "%s" is unknown for %s' % (
                    message_type, self.__class__.__name__))
