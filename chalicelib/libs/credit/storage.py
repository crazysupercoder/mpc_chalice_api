from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.credit.credit import Credit, CreditStorageInterface


class _CreditDynamoDbStorage(DynamoModel, CreditStorageInterface):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'CREDIT'
    
    def __init__(self):
        super(self.__class__, self).__init__(self.TABLE_NAME)

    # ------------------------------------------------------------------------------------------------------------------

    def save(self, credit: Credit) -> None:
        if not isinstance(credit, Credit):
            raise ArgumentTypeException(self.save, 'credit', credit)

        data = {
            'pk': self.PARTITION_KEY,
            'sk': credit.email,
            'earned': credit.earned,
            'paid': credit.paid
        }
        # insert or update
        self.table.put_item(Item=data)

    # ------------------------------------------------------------------------------------------------------------------

    def load(self, email) -> Optional[Credit]:
        if email is None:
            raise ArgumentTypeException(self.load, 'email', email)

        data = self.get_item(email).get('Item', None)
        result = self.__restore(data) if data else None
        return result

    def __restore(self, data) -> Credit:
        email = data.get('sk')

        credit = Credit(
            email,
            float(data.get('earned', '')),
            float(data.get('paid', ''))
        )
        print(credit)
        return credit


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CreditStorageImplementation(CreditStorageInterface):
    def __init__(self):
        self.__storage = _CreditDynamoDbStorage()

    def save(self, credit: Credit) -> None:
        return self.__storage.save(credit)

    def load(self, credit_id) -> Optional[Credit]:
        return self.__storage.load(credit_id)


# ----------------------------------------------------------------------------------------------------------------------

