from chalicelib.extensions import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.models.mpc.Cms.profiles import Profile
from chalicelib.libs.models.mpc.Cms.Informations import Information, InformationAddress, InformationModel, InformationService
from chalicelib.utils.sqs_handlers.base import *


class InformationsSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, informations_request: dict) -> None:
        if not isinstance(informations_request, dict):
            raise ArgumentTypeException(self.__init__, 'informations_request', informations_request)

        self.__informations_request = informations_request

    @classmethod
    def _get_event_type(cls) -> str:
        return 'customer_info_update'

    @property
    def event_data(self) -> dict:
        return self.__informations_request


class InformationsSqsHandler(SqsHandlerInterface):
    def __getInformationModel(self, email: str) -> InformationModel:
        return InformationService().get(email)
    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type
        message_data = sqs_message.message_data

        if message_type == 'customer_info':
            customer = message_data.get('customer')
            if not customer:
                raise ValueError('SQS Message does not have customer field')
            email = str(customer.get('email', '')).strip()
            if not email:
                raise ValueError('SQS Message does not have email field')
            information_model = self.__getInformationModel(email)
            information = information_model.get_information()
            information.first_name = customer.get('first_name')
            information.last_name = customer.get('last_name')
            information.gender = customer.get('gender')
            information_model.insert_item(information)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

