from chalice import Blueprint
from chalicelib.libs.models.mpc.user import User
from chalicelib.extensions import *
from chalicelib.libs.credit.sqs import CreditCashOutSqsSenderEvent, CreditCashOutRequest
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.credit.storage import CreditStorageImplementation


credit_blueprint = Blueprint(__name__)

def __get_current_user() -> User:
    user = credit_blueprint.current_request.current_user
    if user.is_anyonimous:
        raise HttpAuthenticationRequiredException()

    return user


@credit_blueprint.route('/credit_cash_out', methods=['POST'], cors=True)
def credit_cash_out():
    credit_storage = CreditStorageImplementation()

    try:
        user = __get_current_user()
        request_data = credit_blueprint.current_request.json_body

        amount = float(request_data.get('amount', 0))
        if amount == 0:
            raise HttpIncorrectInputDataException()

        account_holder_name = str(request_data.get('account_holder_name', '')).strip()
        account_holder_name = account_holder_name if len(account_holder_name) > 0 else None
        if not account_holder_name:
            raise HttpIncorrectInputDataException()

        account_number = int(request_data.get('account_number', 0))
        if account_number == 0:
            raise HttpIncorrectInputDataException()

        branch_code = str(request_data.get('branch_code', '')).strip()
        branch_code = branch_code if len(branch_code) > 0 else None
        if not branch_code:
            raise HttpIncorrectInputDataException()

        credit = credit_storage.load(user.email)
        if credit.paid < amount:
            raise ValueError('request amount must be equal with paid amount or less than it.')

        credit_cash_out_request = CreditCashOutRequest(
            user.email,
            amount,
            account_holder_name,
            account_number,
            branch_code
        )
        sqs_sender = SqsSenderImplementation()
        event = CreditCashOutSqsSenderEvent(credit_cash_out_request)
        sqs_sender.send(event)

        return {'status': True}
    except BaseException as e:
        return http_response_exception_or_throw(e)


@credit_blueprint.route('/get_credit_info', methods=['GET'], cors=True)
def get_credit_info():
    credit_storage = CreditStorageImplementation()

    try:
        user = __get_current_user()
        credit = credit_storage.load(user.email)
        return {
            'email': credit.email,
            'earned': credit.earned,
            'paid': credit.paid,
        }
    except BaseException as e:
        return http_response_exception_or_throw(e)

