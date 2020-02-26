from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.mailer import MailerImplementation
from .invite_message import InviteMessage

class InviteFriends(object):
    def __init__(
        self,
        sender
    ):
        self.__mailer = MailerImplementation()
        self.__sender = sender

    def send_invite_email(self, to_email):
        message = InviteMessage(self.__sender, to_email)
        self.__mailer.send(message)

