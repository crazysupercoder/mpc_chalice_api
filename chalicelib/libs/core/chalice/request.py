from chalice.app import Request
from ...models.mpc.user import User


class MPCRequest(Request):
    RWS_HEADER_X_TOKEN_NAME = 'x-rws-token'
    RWS_HEADER_SESSION_ID_NAME = 'rws-session-id'
    RWS_HEADER_EMAIL = 'email'
    RWS_HEADER_FIRST_NAME = 'first_name'
    RWS_HEADER_LAST_NAME = 'last_name'

    __current_user__: User = None
    
    @property
    def is_authenticated(self):
        return self.rws_token is not None

    @property
    def current_user(self) -> User:
        if not isinstance(self.__current_user__, User):
            self.__current_user__ = User(
                self.session_id, id=self.rws_token, 
                email=self.rws_email, first_name=self.rws_first_name,
                last_name=self.rws_last_name)
        return self.__current_user__

    @property
    def rws_token(self):
        return self.headers.get(self.RWS_HEADER_X_TOKEN_NAME)

    @property
    def customer_id(self):
        return self.rws_token

    @property
    def session_id(self):
        return self.headers.get(self.RWS_HEADER_SESSION_ID_NAME)

    @property
    def rws_email(self):
        return self.headers.get(self.RWS_HEADER_EMAIL)

    @property
    def rws_first_name(self):
        return self.headers.get(self.RWS_HEADER_FIRST_NAME)

    @property
    def rws_last_name(self):
        return self.headers.get(self.RWS_HEADER_LAST_NAME)

    @property
    def email(self):
        if self.is_authenticated:
            return self.current_user.email
        else:
            return 'BLANK'

    @property
    def page(self):
        if self.query_params is None:
            return 1
        else:
            return int(self.query_params.get('page', 1))

    @property
    def size(self):
        if not self.query_params or not self.query_params.get('size'):
            return 20
        else:
            return int(self.query_params['size'])

    @property
    def gender(self):
        gender = 'UNISEX'
        if self.query_params is not None:
            gender = self.query_params.get('gender', gender).upper()
            if gender.lower() == 'null':
                gender = 'UNISEX'
        elif self.is_authenticated:
            gender = self.current_user.gender
        return gender

    def get_query_parameter(self, param_name, default=None, **kwargs):
        if self.query_params is None:
            return default
        else:
            if self.query_params.get(param_name):
                return self.query_params.get(param_name)
            else:
                return default
