from chalicelib.libs.core.mailer import MailMessageInterface
class InviteMessage(MailMessageInterface):
    def __init__(self, sender, to_email):
        self.__sender = sender
        self.__to_email = to_email

    @property
    def to_email(self) -> str:
        return self.__to_email

    @property
    def content(self) -> str:
        content = """\
        <html>
        <body>
            <p>Hi!
            <br>How are you?<br>
            You have been invited by {}
            </p>
        </body>
        </html>
        """
        content = content.format(self.__sender)
        return content

    @property
    def subject(self) -> str:
        return "Invitation to RunwaySale"

    @property
    def paths_to_attachments(self):
        return None
