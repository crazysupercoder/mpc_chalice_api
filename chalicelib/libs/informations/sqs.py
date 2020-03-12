from chalicelib.extensions import *
from chalicelib.utils.sqs_handlers.base import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface
from chalicelib.libs.models.mpc.Cms.Informations import InformationService
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation, CustomerTierStorageImplementation
from chalicelib.libs.purchase.core import Id


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
    def __init__(self):
        self.__information_service = InformationService()
        self.__customers_storage = CustomerStorageImplementation()
        self.__tiers_storage = CustomerTierStorageImplementation()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type
        message_data = sqs_message.message_data

        if message_type != 'customer_info':
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

        customer_data = message_data.get('customer')
        if not customer_data:
            raise ValueError('SQS Message does not have customer field')

        email = str(customer_data.get('email', '')).strip()
        if not email:
            raise ValueError('SQS Message does not have email field')

        information_model = self.__information_service.get(email)
        information = information_model.get_information()
        information.first_name = customer_data.get('first_name')
        information.last_name = customer_data.get('last_name')
        information.gender = customer_data.get('gender')
        information_model.insert_item(information)

        # set tier
        tier_id = customer_data['mpc_tier_id']
        tier = self.__tiers_storage.get_by_id(Id(str(tier_id)))
        if not tier:
            raise ValueError('Unable to change Tier #{} for Customer #{} - tier does not exist!'.format(
                tier_id,
                information.customer_id
            ))

        customer = self.__customers_storage.get_by_id(Id(information.customer_id))
        customer.tier = tier
        self.__customers_storage.save(customer)

