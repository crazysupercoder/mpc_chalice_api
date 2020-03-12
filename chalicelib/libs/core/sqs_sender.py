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

    def send_batch(self, events: List[SqsSenderEventInterface]) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------
#                                           IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class SqsSenderImplementation(SqsSenderInterface):
    def __init__(self) -> None:
        self.__sender: SqsSenderInterface = create_object(settings.SQS_SENDER_CONFIG.get('class'))

    def send(self, event: SqsSenderEventInterface) -> None:
        if not isinstance(event, SqsSenderEventInterface):
            raise ArgumentTypeException(self.send, 'event', event)

        self.__sender.send(event)

    def send_batch(self, events: List[SqsSenderEventInterface]) -> None:
        if not isinstance(events, list) or any(
                not isinstance(x, SqsSenderEventInterface) for x in events):
            raise ArgumentTypeException(
                self.send_batch, 'event',
                [
                    event for event in events
                    if not isinstance(event, SqsSenderEventInterface)
                ][0])
        
        self.__sender.send_batch(events)


class _SqsSenderDummyPrint(SqsSenderInterface):
    def send_batch(self, events: List[SqsSenderEventInterface]) -> None:
        for event in events:
            self.send(event)

    def send(self, event: SqsSenderEventInterface) -> None:
        print('\r\n\r\n\r\n')
        print(self.__class__.__qualname__, event.event_type, event.event_data)
        print('\r\n\r\n\r\n')


class _SqsSenderSqs(SqsSenderInterface):
    def __init__(self):
        self.__sqs_client = boto3.client('sqs')
        self.__logger = Logger()

    def send_batch(self, events: List[SqsSenderEventInterface]) -> None:
        # Group by event_type
        grouped = dict()
        for event in events:
            if not grouped.get(event.event_type):
                grouped[event.event_type] = list()
            grouped[event.event_type].append(event.event_data)

        CHUNK_SIZE = settings.CALCULATE_SCORE_CHUNK_SIZE
        for event_type, event_data in grouped.items():
            events_map = settings.SQS_SENDER_CONFIG.get('params').get('events')
            queue_data = events_map.get(event_type) or None

            if not queue_data:
                raise ArgumentValueException('{} does not know, how to send event!'.format(
                    self.send_batch.__qualname__))

            queue_url = queue_data.get('queue_url')
            # queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url.split('/')[-1])

            for idx in range(0, len(event_data), CHUNK_SIZE):
                batch = event_data[idx: idx + CHUNK_SIZE]

                def __log_flow(msg: str, event_type: str = event_type, data: list = batch):
                    self.__logger.log_simple('{} : Sending SQS "{}" : {} : {}'.format(
                        self.__class__.__qualname__,
                        event_type, data, msg))

                __log_flow('Start')

                __log_flow('SQS Point: {} -> {}'.format(event_type, queue_url))

                send_method = self.__send_fifo if str(queue_url)[-5:] == '.fifo' else self.__send_standard
                send_method(queue_url, event_type, batch, __log_flow)

                __log_flow('End')

    def send(self, event: SqsSenderEventInterface) -> None:
        def __log_flow(text: str) -> None:
            self.__logger.log_simple('{} : Sending SQS "{}" : {} : {}'.format(
                self.__class__.__qualname__,
                event.event_type,
                event.event_data,
                text
            ))

        __log_flow('Start')

        events_map = settings.SQS_SENDER_CONFIG.get('params').get('events')
        queue_data = events_map.get(event.event_type) or None
        if not queue_data:
            raise ArgumentValueException('{} does not know, how to send "{}" event!'.format(
                self.send.__qualname__,
                event.event_type
            ))

        queue_url = queue_data.get('queue_url')
        object_type = queue_data.get('object_type')
        send_method = self.__send_fifo if str(queue_url)[-5:] == '.fifo' else self.__send_standard

        __log_flow('SQS Point: {} -> {}'.format(object_type, queue_url))

        send_method(queue_url, object_type, event.event_data, __log_flow)

        __log_flow('End')

    def __send_standard(self, queue_url: str, object_type: str, data: dict, __log_flow):
        __log_flow('Standard - Start')
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
        __log_flow('Standard - End: {}'.format(response))

    def __send_fifo(self, queue_url: str, object_type: str, data: dict, __log_flow):
        __log_flow('Fifo - Start')
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
        __log_flow('Fifo - End: {}'.format(response))


# ----------------------------------------------------------------------------------------------------------------------

