from chalicelib.extensions import *
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface
from chalicelib.libs.contactus.contactus_request import ContactusRequest
from chalicelib.utils.sqs_handlers.base import *

class ContactusSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, contactus_request: ContactusRequest) -> None:
        if not isinstance(contactus_request, ContactusRequest):
            raise ArgumentTypeException(self.__init__, 'contactus_request', contactus_request)

        self.__contactus_request = contactus_request

    @classmethod
    def _get_event_type(cls) -> str:
        return 'contactus_request'

    @property
    def event_data(self) -> dict:
        return {
            'issue': self.__contactus_request.issue,
            'issue_detail': self.__contactus_request.issue_detail,
            'first_name': self.__contactus_request.first_name,
            'last_name': self.__contactus_request.last_name,
            'phone': self.__contactus_request.phone,
            'email': self.__contactus_request.email,
            'subject': self.__contactus_request.subject,
            'message': self.__contactus_request.message,
            'file_url': self.__contactus_request.file_url
        }

