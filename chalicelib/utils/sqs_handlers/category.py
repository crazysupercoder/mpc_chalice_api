from .base import *
from chalicelib.libs.models.mpc.categories import Category


class CategorySqsHandler(SqsHandlerInterface):
    def handle(self, sqs_message: SqsMessage) -> None:
        Category().delete_item(sqs_message.message_data['id'])

