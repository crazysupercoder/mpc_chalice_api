import json
import boto3
import hashlib
import datetime
from typing import List, Union
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.logger import Logger


# ----------------------------------------------------------------------------------------------------------------------
#                                               INTERFACE
# ----------------------------------------------------------------------------------------------------------------------


class SqsSenderEventInterface(object):
    @classmethod
    def _get_event_type(cls) -> str:
        raise NotImplementedError()

    @property
    def event_type(self) -> str:
        return self.__class__._get_event_type()

    @property
    def event_data(self) -> dict:
        raise NotImplementedError()


class SqsSenderInterface(object):
    def send(self, event: SqsSenderEventInterface) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------
#                                           IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class SqsSenderImplementation(SqsSenderInterface):
    def __init__(self) -> None:
        self.__sender: SqsSenderInterface = create_object(settings.SQS_SENDER_CONFIG.get('class'))

    def send(self, event: Union[SqsSenderEventInterface, List[SqsSenderEventInterface]]) -> None:
        if not isinstance(event, (SqsSenderEventInterface, list)):
            raise ArgumentTypeException(self.send, 'event', event)
        elif isinstance(event, list) and not all(isinstance(x, SqsSenderEventInterface) for x in event):
            raise ArgumentTypeException(self.send, 'event', event[0])

        self.__sender.send(event)


class _SqsSenderDummyPrint(SqsSenderInterface):
    def send(self, event: SqsSenderEventInterface) -> None:
        print('\r\n\r\n\r\n')
        print(self.__class__.__qualname__, event.event_type, event.event_data)
        print('\r\n\r\n\r\n')


class _SqsSenderSqs(SqsSenderInterface):
    def __init__(self):
        self.__sqs_client = boto3.client('sqs')
        self.__logger = Logger()

    def send(self, event: Union[SqsSenderEventInterface, List[SqsSenderEventInterface]]) -> None:
        if isinstance(event, SqsSenderEventInterface):
            event_type = event.event_type
            event_data = event.event_data
        elif isinstance(event, list) and all(isinstance(x, SqsSenderEventInterface) for x in event):
            event_type = event[0].event_type
            event_data: List[dict] = [item.event_data for item in event]

        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : Sending SQS "{}" : {} : {}'.format(
                self.__class__.__qualname__,
                event_type,
                event_data,
                text
            ))

        __log_flow('Start')

        events_map = settings.SQS_SENDER_CONFIG.get('params').get('events')
        queue_data = events_map.get(event_type) or None
        if not queue_data:
            raise ArgumentValueException('{} does not know, how to send "{}" event!'.format(
                self.send.__qualname__,
                event_type
            ))

        queue_url = queue_data.get('queue_url')
        object_type = queue_data.get('object_type')
        send_method = self.__send_fifo if str(queue_url)[-5:] == '.fifo' else self.__send_standard

        __log_flow('SQS Point: {} -> {}'.format(object_type, queue_url))

        send_method(queue_url, object_type, event_data, __log_flow)

        __log_flow('End')

    def __send_standard(
            self, queue_url: str, object_type: str,
            data: Union[dict, List[dict]], __log_flow):
        __log_flow('Standard - Start')
        if isinstance(data, dict):
            response = self.__sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(data),
                DelaySeconds=45,
                MessageAttributes={
                    'object_type': {
                        'StringValue': object_type,
                        'DataType': 'String',
                    }
                }
            )
        else:
            response = self.__sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=[{
                    'Id': hashlib.md5(item.get('email').encode("UTF-8")).hexdigest(),
                    'MessageBody': json.dumps(item),
                    'DelaySeconds': 45,
                    'MessageAttributes': {
                        'object_type': {
                            'StringValue': object_type,
                            'DataType': 'String',
                        }
                    }
                } for item in data]
            )
        __log_flow('Standard - End: {}'.format(response))

    def __send_fifo(self, queue_url: str, object_type: str, data: Union[dict, List[dict]], __log_flow):
        __log_flow('Fifo - Start')
        if isinstance(data, dict):
            response = self.__sqs_client.send_message(
                QueueUrl=queue_url,
                MessageGroupId=object_type,
                MessageDeduplicationId=hashlib.md5((
                    object_type
                    + json.dumps(data)
                    + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                ).encode('utf-8')).hexdigest(),
                MessageBody=json.dumps(data),
                MessageAttributes={
                    'object_type': {
                        'StringValue': object_type,
                        'DataType': 'String',
                    }
                }
            )
        else:
            raise NotImplementedError("")
        __log_flow('Fifo - End: {}'.format(response))


# ----------------------------------------------------------------------------------------------------------------------

