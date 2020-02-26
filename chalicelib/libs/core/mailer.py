import os
import re
from typing import Tuple
from chalicelib.extensions import *
from chalicelib.settings import settings
from mailer import Mailer as _SmtpMailerLib
from mailer import Message as _SmtpMailerLibMessage


# ----------------------------------------------------------------------------------------------------------------------
#                                               INTERFACE
# ----------------------------------------------------------------------------------------------------------------------


class MailMessageInterface(object):
    @property
    def to_email(self) -> str:
        raise NotImplementedError()

    @property
    def subject(self) -> str:
        raise NotImplementedError()

    @property
    def content(self) -> str:
        raise NotImplementedError()

    @property
    def paths_to_attachments(self) -> Tuple[str]:
        raise NotImplementedError()


class MailerInterface(object):
    def send(self, message: MailMessageInterface) -> None:
        """
        :param message:
        :return:
        :raises MailerMessageIncorrectException:
        """

        if not isinstance(message, MailMessageInterface):
            raise ArgumentTypeException(self.send, 'message', message)

        to_email = message.to_email
        subject = message.subject
        html_content = message.content
        paths_to_attachments = message.paths_to_attachments

        for required_value in [to_email, subject, html_content]:
            if not required_value or not str(required_value).strip():
                raise MailerMessageIncorrectException(message)

        if paths_to_attachments is not None:
            for path_to_attachment in paths_to_attachments:
                path_to_attachment = str(path_to_attachment).strip() if path_to_attachment else None
                if not path_to_attachment or not os.path.isfile(path_to_attachment):
                    raise MailerMessageIncorrectException(message)

        self._send(message)

    def _send(self, mail_message: MailMessageInterface) -> None:
        raise not NotImplementedError()


class MailerMessageIncorrectException(ValueError):
    def __init__(self, message: MailMessageInterface):
        super().__init__('Unable to Send Mail: Message "{}" is incorrect!'.format({
            'to_email': message.to_email,
            'subject': message.subject,
            'content': message.content,
            'paths_to_attachments': message.paths_to_attachments,
        }))


# ----------------------------------------------------------------------------------------------------------------------
#                                           IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class MailerImplementation(MailerInterface):
    def __init__(self):
        self.__mailer: MailerInterface = create_object(settings.MAILER_CONFIG.get('class'))

    def send(self, message: MailMessageInterface):
        self.__mailer.send(message)


class _MailerDummyPrint(MailerInterface):
    def _send(self, mail_message: MailMessageInterface) -> None:
        print('\r\n\r\n\r\n')
        print(
            self.send.__qualname__,
            mail_message.to_email,
            mail_message.subject,
            mail_message.content,
            mail_message.paths_to_attachments
        )
        print('\r\n\r\n\r\n')


class _MailerSmtp(MailerInterface):
    def _send(self, mail_message: MailMessageInterface) -> None:
        from_email = settings.MAILER_CONFIG.get('params').get('from_email')
        to_email = mail_message.to_email
        subject = mail_message.subject
        html_content = mail_message.content
        alternate_content = self.__create_alternate_content(mail_message.content)
        paths_to_attachments = mail_message.paths_to_attachments

        message = _SmtpMailerLibMessage(From=from_email, To=to_email)
        message.Subject = subject
        message.Html = html_content
        message.Body = alternate_content
        if paths_to_attachments is not None:
            for path_to_attachment in paths_to_attachments:
                message.attach(path_to_attachment)

        sender = _SmtpMailerLib(
            host=settings.MAILER_CONFIG.get('params').get('host'),
            port=settings.MAILER_CONFIG.get('params').get('port'),
            usr=settings.MAILER_CONFIG.get('params').get('username'),
            pwd=settings.MAILER_CONFIG.get('params').get('password')
        )
        sender.send(message)

    def __create_alternate_content(self, html_content):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', html_content)


# ----------------------------------------------------------------------------------------------------------------------

