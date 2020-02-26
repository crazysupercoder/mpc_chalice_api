from typing import Tuple, List
from chalice import Blueprint, UnauthorizedError, NotFoundError, BadRequestError
from chalicelib.libs.message.base import Message, MessageStorageImplementation

blueprint = Blueprint(__name__)


def __response_list(messages: Tuple[Message]) -> Tuple[dict]:
    response: List[dict] = [{
        'id': message.message_id,
        'title': message.title,
        'text': message.text,
        'created_at': message.created_at.strftime('%Y-%m-%d %H:%M:%S')
    } for message in messages]

    return tuple(response)


@blueprint.route('/list', methods=['GET'], cors=True)
def messages_list():
    user = blueprint.current_request.current_user
    if user.is_anyonimous:
        raise UnauthorizedError('Authentication is required!')

    message_storage = MessageStorageImplementation()
    messages = message_storage.get_all_for_customer(user.email)

    return __response_list(messages)


@blueprint.route('/dismiss', methods=['DELETE'], cors=True)
def messages_dismiss():
    user = blueprint.current_request.current_user
    if user.is_anyonimous:
        raise UnauthorizedError('Authentication is required!')

    message_id = str(blueprint.current_request.json_body.get('message_id') or '').strip() or None
    if not message_id:
        raise BadRequestError('"message_id" is required!')

    message_storage = MessageStorageImplementation()

    messages = list(message_storage.get_all_for_customer(user.email))
    for k in range(0, len(messages)):
        if messages[k].message_id == message_id:
            message_storage.remove(message_id)
            del messages[k]
            break
    else:
        raise NotFoundError('Your message #{} was not found!'.format(message_id))

    return __response_list(tuple(messages))

