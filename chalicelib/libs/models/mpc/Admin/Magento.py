import boto3
import hashlib
import json
from requests import post
from .....settings import settings
from ..base import Base
from chalicelib.libs.core.logger import Logger

class Magento(Base):
    table = None
    PARTITION_KEY = 'CUSTOMER'

    def __init__(self):
        Base.__init__(self)
        self.table = self.dynamodb.Table(
            settings.AWS_DYNAMODB_MAGENTO_CUSTOMER_TABLE_NAME)

    def insert(self, item):
        return self.table.put_item(Item=item)

    def user_by_email(self, email):
        return self.get(email)[0]

    def get(self, email: str, password: str, check_dynamo: bool = False) -> bool:
        if check_dynamo:
            items = self.table.query(
                KeyConditionExpression="pk = :pk and sk = :sk",
                ExpressionAttributeValues={":pk": self.PARTITION_KEY, ":sk": email}
            )['Items']
            if len(items) > 0:
                item = items[0]
                current_pwd_hash = item['hash']
                salt = ""
                if current_pwd_hash.find(":") != -1:
                    salt = current_pwd_hash[current_pwd_hash.find(":") + 1:]
                    current_pwd_hash = current_pwd_hash[:current_pwd_hash.find(":")]

                hs = hashlib.md5((salt + password).encode())
                coded_hash = hs.hexdigest()

                if coded_hash == current_pwd_hash:
                    return True
                else:
                    return False
            else:
                return False
        else:
            # TODO: This is the snippet to invoke Magento auth API.

            try:
                url = 'https://portal.runway.co.za/api/remoteAuth/customer'
                body = {'email': email, 'password': password}
                headers = {'Identification': 'RunwaySale::ReadAPI'}
                r = post(url, data=body, headers=headers)

                if r.status_code == 200:
                    result = json.loads(r.text)
                    if not result['status']:
                        Logger().log_simple(', '.join([email, password, result['error']]))
                    return result['status']
                else:
                    return False
            except BaseException as e:
                Logger().log_exception(e)
                return False

    def validate_user(self, email, password, check_dynamo: bool = False):
        created = False
        found = False
        msg = None
        if not email or not password:
            return found, created, 'email and password is required.'
        else:
            try:
                found_user = self.get(email, password, check_dynamo=check_dynamo)
                if found_user:
                    found = True

                    # TODO: Should create a new user in cognito
                    created, msg = self.create_cognito_user(email, password)

                    # request customer info
                    try:
                        # @todo : this is a crutch
                        # This should be an api request, but it is impossible for now,
                        # so we use sqs-communication.
                        from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
                        from chalicelib.libs.purchase.customer.sqs import CrutchCustomerInfoRequestSqsSenderEvent
                        event = CrutchCustomerInfoRequestSqsSenderEvent(user.email)
                        SqsSenderImplementation().send(event)
                    except BaseException as e:
                        Logger().log_exception(e)

                    else:
                        msg = 'Incorrect username or password.'
            except Exception as e:
                msg = str(e)

        return found, created, msg

    def create_cognito_user(
            self, email, password, first_name=None, last_name=None, **kwargs):
        client = boto3.client('cognito-idp')
        try:
            response = client.admin_create_user(
                UserPoolId=settings.AWS_COGNITO_USER_POOL_ID,
                Username=email,
                # UserAttributes=[
                #     {
                #         'Name': 'first_name',
                #         'Value': 'string'
                #     },
                # ],
                # ValidationData=[
                #     {
                #         'Name': 'string',
                #         'Value': 'string'
                #     },
                # ],
                # TemporaryPassword='string',
                # ForceAliasCreation=False,
                MessageAction='SUPPRESS',
                DesiredDeliveryMediums=['EMAIL']
            )
            if response.get('User') is None:
                return False, 'Unexpected issue'
            
            return self.update_cognito_user_password(email, password)
        except Exception as e:
            return False, str(e)

    def update_cognito_user_password(self, email, new_password, permenant=True):
        client = boto3.client('cognito-idp')
        try:
            response = client.admin_set_user_password(
                UserPoolId=settings.AWS_COGNITO_USER_POOL_ID,
                Username=email,
                Password=new_password,
                Permanent=permenant
            )
            return True, ''
        except Exception as e:
            return False, str(e)
