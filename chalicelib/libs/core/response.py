import json
from ...settings import settings


def build_response(status_code, body, request=None):
    if settings.DEBUG and request is not None:
        body['request'] = request
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True
        },
        'body': body
    }


def return_success(body, request=None):
    return build_response(200, body, request=request)


def return_failure(body, request=None):
    return build_response(500, body, request=request)
