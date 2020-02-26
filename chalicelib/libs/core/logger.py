import datetime
import traceback
from chalicelib.extensions import *
from chalicelib.settings import settings


class _LogWriterInterface(object):
    def write(self, message: str) -> None:
        raise NotImplementedError()


class _PrintLogWriter(_LogWriterInterface):
    def write(self, message: str) -> None:
        print(message)


class Logger(object):
    def __init__(self):
        # @todo : should be configurable, but hardcoded implementation is enough for now
        self.__writer: _LogWriterInterface = _PrintLogWriter()

    def __log_message(self, message: str) -> None:
        message = '[{}] : {}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message)
        self.__writer.write(message)

    def log_exception(self, e: BaseException) -> None:
        if not isinstance(e, BaseException):
            raise ArgumentTypeException(self.log_exception, 'e', e)

        self.__log_message(str(e) + str(traceback.format_exc()))

    def log_simple(self, message: str) -> None:
        message = str(message)
        self.__log_message(message)

