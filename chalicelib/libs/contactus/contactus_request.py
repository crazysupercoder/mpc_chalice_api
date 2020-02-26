import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *

# ----------------------------------------------------------------------------------------------------------------------

class ContactusRequest(object):
    def __init__(
        self,
        issue: str,
        issue_detail: str,
        first_name: str,
        last_name: str,
        phone: str,
        email: str,
        subject: str,
        message: str,
        file_url: str
    ) -> None:
        if not isinstance(issue, str):
            raise ArgumentTypeException(self.__init__, 'issue', issue)

        if not isinstance(issue_detail, str):
            raise ArgumentTypeException(self.__init__, 'issue_detail', issue_detail)

        if not isinstance(first_name, str):
            raise ArgumentTypeException(self.__init__, 'first_name', first_name)

        if not isinstance(last_name, str):
            raise ArgumentTypeException(self.__init__, 'last_name', last_name)

        if not isinstance(phone, str):
            raise ArgumentTypeException(self.__init__, 'phone', phone)

        if not isinstance(email, str):
            raise ArgumentTypeException(self.__init__, 'email', email)

        if not isinstance(subject, str):
            raise ArgumentTypeException(self.__init__, 'subject', subject)
        
        if not isinstance(message, str):
            raise ArgumentTypeException(self.__init__, 'message', message)

        if not isinstance(file_url, str):
            raise ArgumentTypeException(self.__init__, 'file_url', file_url)

        self.__issue = issue
        self.__issue_detail = issue_detail
        self.__first_name = first_name
        self.__last_name = last_name
        self.__phone = phone
        self.__email = email
        self.__subject = subject
        self.__message = message
        self.__file_url = file_url
        
    @property
    def issue(self) -> str:
        return self.__issue

    @property
    def issue_detail(self) -> float:
        return self.__issue_detail
        
    @property
    def first_name(self) -> str:
        return self.__first_name
        
    @property
    def last_name(self) -> int:
        return self.__last_name
        
    @property
    def phone(self) -> str:
        return self.__phone
        
    @property
    def email(self) -> str:
        return self.__email
        
    @property
    def subject(self) -> int:
        return self.__subject
        
    @property
    def message(self) -> str:
        return self.__message

    @property
    def file_url(self) -> str:
        return self.__file_url