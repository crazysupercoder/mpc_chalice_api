from .base import SqsMessage, SqsHandlerInterface
from chalicelib.libs.models.mpc.Cms.StaticPages import StaticPage, StaticPageStorage


class StaticPageSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__storage = StaticPageStorage()

    def handle(self, sqs_message: SqsMessage) -> None:
        if sqs_message.message_type == 'static_page_publish':
            static_page = StaticPage(
                sqs_message.message_data.get('descriptor'),
                sqs_message.message_data.get('name'),
                sqs_message.message_data.get('content')
            )
            self.__storage.save(static_page)
        elif sqs_message.message_type == 'static_page_unpublish':
            self.__storage.remove(sqs_message.message_data.get('descriptor'))
        else:
            raise Exception('{} does not know, how to process "{}" message: {}!'.format(
                self.__class__.__qualname__,
                sqs_message.message_type,
                sqs_message.message_data
            ))

