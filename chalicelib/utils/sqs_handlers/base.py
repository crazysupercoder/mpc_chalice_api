
class SqsMessage(object):
    def __init__(self, message_id: str, message_type: str, message_data: dict):
        self.__message_id = message_id
        self.__message_type = message_type
        self.__message_data = message_data

    @property
    def id(self) -> str:
        return self.__message_id

    @property
    def message_type(self) -> str:
        return self.__message_type

    @property
    def message_data(self) -> dict:
        return self.__message_data


# ----------------------------------------------------------------------------------------------------------------------


class SqsHandlerInterface(object):
    def handle(self, sqs_message: SqsMessage) -> None:
        raise NotImplementedError()

