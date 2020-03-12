from .base import *
from chalicelib.constants.sqs import SCORED_PRODUCT_MESSAGE_TYPE
from chalicelib.libs.models.mpc.Cms.meta import Meta
from chalicelib.libs.models.ml.scored_products import ScoredProduct


class ScoredProductSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__product_model = ScoredProduct()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER:
            if isinstance(sqs_message.message_data, dict):
                self.__product_model.calculate_scores(**sqs_message.message_data)
            elif isinstance(sqs_message.message_data, list) and\
                    all(isinstance(x, dict) for x in sqs_message.message_data):
                for item in sqs_message.message_data:
                    self.__product_model.calculate_scores(**item)
            else:
                print("Unknown case - %s" % sqs_message.message_data)
        elif message_type == SCORED_PRODUCT_MESSAGE_TYPE.SECRET_KEY:
            meta = Meta()
            meta.secret_key = sqs_message.message_data.get('value')
        else:
            raise ValueError(
                'SQS Message type "%s" is unknown for %s' % (
                    message_type, self.__class__.__name__))
