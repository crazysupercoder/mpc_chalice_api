from chalice import CognitoUserPoolAuthorizer, IAMAuthorizer
from ...settings import settings


cognito_authorizer = CognitoUserPoolAuthorizer(
    settings.AWS_COGNITO_USER_POOL_NAME,
    header='Authorization',
    provider_arns=[settings.AWS_COGNITO_USER_POOL_ARN])

iam_authorizer = IAMAuthorizer()
