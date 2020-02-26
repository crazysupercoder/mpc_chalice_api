from chalicelib.extensions import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface
from chalicelib.libs.credit.credit import CreditCashOutRequest, Credit
from chalicelib.libs.credit.storage import CreditStorageImplementation
from chalicelib.utils.sqs_handlers.base import *


class CreditCashOutSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, credit_cash_out_request: CreditCashOutRequest) -> None:
        if not isinstance(credit_cash_out_request, CreditCashOutRequest):
            raise ArgumentTypeException(self.__init__, 'credit_cash_out_request', credit_cash_out_request)

        self.__credit_cash_out_request = credit_cash_out_request

    @classmethod
    def _get_event_type(cls) -> str:
        return 'credit_cash_out_request'

    @property
    def event_data(self) -> dict:
        return {
            'email': self.__credit_cash_out_request.email,
            'amount': self.__credit_cash_out_request.amount,
            'account_holder_name': self.__credit_cash_out_request.account_holder_name,
            'account_number': self.__credit_cash_out_request.account_number,
            'branch_code': self.__credit_cash_out_request.branch_code
        }


class UserCreditBalanceSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__credit_storage = CreditStorageImplementation()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type
        message_data = sqs_message.message_data

        if message_type == 'user_credit_balance':
            credit = Credit(message_data['email'], float(message_data['earned']), float(message_data['paid']))
            self.__credit_storage.save(credit)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

