from chalicelib.extensions import *
from chalice import Blueprint, BadRequestError
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.subscription.subscription import Id, Email, SubscriptionStorage, SubscriptionService


blueprint = Blueprint(__name__)


@blueprint.route('subscribe', methods=['POST'], cors=True)
def subscribe():
    request = blueprint.current_request

    try:
        email = Email(str(request.json_body.get('email') or '').strip() or None)
    except (TypeError, ValueError):
        raise BadRequestError('Incorrect Input Data!')

    try:
        user_id = Id(request.customer_id) if request.is_authenticated else None
        subscription_service = SubscriptionService(SubscriptionStorage(), SqsSenderImplementation())
        subscription_service.subscribe(email, user_id)
    except ApplicationLogicException as e:
        return {
            'status': False,
            'message': str(e)
        }

    return {
        'status': True,
        'message': 'Successfully Subscribed!'
    }


@blueprint.route('unsubscribe', methods=['DELETE'], cors=True)
def unsubscribe():
    request = blueprint.current_request

    try:
        subscription_id = Id(str(request.json_body.get('subscription_id') or '').strip() or None)
    except (TypeError, ValueError):
        raise BadRequestError('Incorrect Input Data!')

    try:
        subscription_service = SubscriptionService(SubscriptionStorage(), SqsSenderImplementation())
        subscription_service.unsubscribe(subscription_id)
    except ApplicationLogicException as e:
        return {
            'status': False,
            'message': str(e)
        }

    return {
        'status': True,
        'message': 'Successfully Unsubscribed!'
    }

