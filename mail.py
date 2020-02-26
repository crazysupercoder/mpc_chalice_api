import smtplib, ssl, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from chalicelib.settings import Config
config = Config()
receiver_email = ""

SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
SMTP_HOST = os.environ.get('SMTP_HOST')
SMTP_PORT = os.environ.get('SMTP_PORT')
SMTP_USERNAME = os.environ.get('SMTP_USERNAME')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')

message = MIMEMultipart("alternative")
message["Subject"] = "multipart test"
message["From"] = SENDER_EMAIL
message["To"] = receiver_email

# Create the plain-text and HTML version of your message
text = """\
Hi,
How are you?
Please visit:
storybook-dev.mpc.runway.co.za"""
html = """\
<html>
  <body>
    <p>Hi,<br>
       How are you?<br>
       <a href="http://storybook-dev.mpc.runway.co.za">RunwaySale</a> 
       Please visit.
    </p>
  </body>
</html>
"""

# Turn these into plain/html MIMEText objects
part1 = MIMEText(text, "plain")
part2 = MIMEText(html, "html")

# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
message.attach(part1)
message.attach(part2)

server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
server.starttls()
server.set_debuglevel(1)
server.login(SMTP_USERNAME, SMTP_PASSWORD)
server.sendmail(
    SENDER_EMAIL, receiver_email, text
)
server.quit()