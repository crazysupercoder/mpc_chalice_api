from chalice import Blueprint

blueprint = Blueprint(__name__)


@blueprint.route('version', methods=['GET'], cors=True)
def version():
    import os
    import datetime

    def __get_last_modified_at(path) -> datetime.datetime:
        max_modified_at = None

        if os.path.isdir(path):
            for file_name in os.listdir(path):
                if file_name in ('__pycache__',):
                    continue

                inner_path = (path[:-1] if path[-1] == '/' else path) + '/' + file_name
                inner_path_last_modified_at = __get_last_modified_at(inner_path)
                max_modified_at = max(max_modified_at or inner_path_last_modified_at, inner_path_last_modified_at)

            max_modified_at = max_modified_at or datetime.datetime.fromtimestamp(os.stat(path).st_mtime)
        elif os.path.isfile(path):
            max_modified_at = datetime.datetime.fromtimestamp(os.stat(path).st_mtime)
        else:
            raise Exception('{} does not know, how to work with {}'.format(
                str(__file__) + __get_last_modified_at.__qualname__,
                path
            ))

        return max_modified_at

    path_src_root = os.path.dirname(os.path.abspath(__file__)) + '/../../'
    src_last_modified_at = __get_last_modified_at(path_src_root)

    path_app_file = path_src_root + '../app.py'
    app_file_last_modified_at = __get_last_modified_at(path_app_file)

    last_modified_at = max(src_last_modified_at, app_file_last_modified_at)

    return {
        'version': last_modified_at.strftime('%Y%m%d%H%M%S'),
    }


@blueprint.route('check-sqs/{queue_url}', methods=['GET'], cors=True)
def check_sqs(queue_url):
    from urllib.parse import unquote
    import boto3
    import json
    import hashlib

    object_type = 'check_sqs'
    json_data = '%7B%22check%22%3A%22sqs%22%7D'

    try:
        queue_url = unquote(queue_url)
        object_type = unquote(object_type)
        json_data = unquote(json_data)

        sqs_client = boto3.client('sqs')

        def __send_standard():
            return sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(json_data),
                DelaySeconds=45,
                MessageAttributes={
                    'object_type': {
                        'StringValue': object_type,
                        'DataType': 'String',
                    }
                }
            )

        def __send_fifo():
            return sqs_client.send_message(
                QueueUrl=queue_url,
                MessageGroupId='check_sqs',
                MessageDeduplicationId=hashlib.md5(json.dumps(json_data).encode('utf-8')).hexdigest(),
                MessageBody=json.dumps(json_data),
                MessageAttributes={
                    'object_type': {
                        'StringValue': object_type,
                        'DataType': 'String',
                    }
                }
            )

        send_method = __send_fifo if str(queue_url)[-5:] == '.fifo' else __send_standard
        sqs_response = send_method()
    except BaseException as e:
        return {
            'error': str(e),
            'url': queue_url,
            'object_type': object_type,
            'data': json_data,
        }

    return {
        'url': queue_url,
        'object_type': object_type,
        'data': json_data,
        'sqs_response': sqs_response
    }
