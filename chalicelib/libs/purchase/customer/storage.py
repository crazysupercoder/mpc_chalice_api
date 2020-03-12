import json
from decimal import Decimal
from typing import Optional, Tuple
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.purchase.core import \
    Id, Name, Percentage, \
    CustomerInterface, CustomerStorageInterface, \
    CustomerTier, CustomerTierStorageInterface
from chalicelib.libs.purchase.customer.customer import CustomerImplementation
from chalicelib.libs.models.mpc.Cms.profiles import InformationModel
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector


class _CustomerTiersElasticStorage(CustomerTierStorageInterface):
    __ENTITY_PROPERTY_ID = '__id'
    __ENTITY_PROPERTY_NAME = '__name'
    __ENTITY_PROPERTY_CREDIT_BACK_PERCENT = '__credit_back_percent'
    __ENTITY_PROPERTY_SPENT_AMOUNT_MIN = 'spent_amount_min'
    __ENTITY_PROPERTY_SPENT_AMOUNT_MAX = 'spent_amount_max'
    __ENTITY_PROPERTY_IS_DELETED = '__is_deleted'

    def __init__(self):
        """
        curl -X DELETE localhost:9200/customer_tiers_tiers
        curl -X PUT localhost:9200/customer_tiers_tiers -H "Content-Type: application/json" -d'{
            "mappings": {
                "customer_tiers_tiers": {
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "keyword"},
                        "credit_back_percent": {"type": "integer"},
                        "spent_amount_min": {"type": "integer"},
                        "spent_amount_max": {"type": "integer"},
                        "is_deleted": {"type": "boolean"}
                    }
                }
            }
        }'
        """
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_TIERS,
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_TIERS
        )
        self.__reflector = Reflector()

    def save(self, entity: CustomerTier) -> None:
        entity_data = self.__reflector.extract(entity, [
            self.__class__.__ENTITY_PROPERTY_ID,
            self.__class__.__ENTITY_PROPERTY_NAME,
            self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT,
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN,
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX,
            self.__class__.__ENTITY_PROPERTY_IS_DELETED
        ])

        document_id = entity_data[self.__class__.__ENTITY_PROPERTY_ID].value
        document_data = {
            'id': entity_data[self.__class__.__ENTITY_PROPERTY_ID].value,
            'name': entity_data[self.__class__.__ENTITY_PROPERTY_NAME].value,
            'credit_back_percent': entity_data[self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT].value,
            'spent_amount_min': entity_data[self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN],
            'spent_amount_max': entity_data[self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX],
            'is_deleted': entity_data[self.__class__.__ENTITY_PROPERTY_IS_DELETED],
        }

        if self.__elastic.get_data(document_id):
            self.__elastic.update_data(document_id, {'doc': document_data})
        else:
            self.__elastic.create(document_id, document_data)

    def __create_entity(self, row: dict) -> CustomerTier:
        entity = self.__reflector.construct(CustomerTier, {
            self.__class__.__ENTITY_PROPERTY_ID: Id(str(row['id'])),
            self.__class__.__ENTITY_PROPERTY_NAME: Name(row['name']),
            self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT: Percentage(int(row['credit_back_percent'])),
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN: int(row['spent_amount_min']),
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX: int(row['spent_amount_max']),
            self.__class__.__ENTITY_PROPERTY_IS_DELETED: row['is_deleted'],
        })
        return entity

    def get_by_id(self, tier_id: Id) -> Optional[CustomerTier]:
        if not isinstance(tier_id, Id):
            raise ArgumentTypeException(self.get_by_id, 'tier_id', tier_id)

        row = self.__elastic.get_data(tier_id.value)
        return self.__create_entity(row) if row else None

    def get_all(self) -> Tuple[CustomerTier]:
        rows = self.__elastic.post_search({'query': {'match_all': {}}}).get('hits', {}).get('hits')
        result = [self.__create_entity(row['_source']) for row in rows]
        result = [entity for entity in result if not entity.is_deleted]
        result = tuple(result)
        return result

    def get_neutral(self) -> CustomerTier:
        for tier in self.get_all():
            if tier.is_neutral:
                return tier
        else:
            raise ApplicationLogicException('Neutral Tier does not exist!')


class _CustomerTiersStorageDynamoDb(CustomerTierStorageInterface):
    __ENTITY_PROPERTY_ID = '__id'
    __ENTITY_PROPERTY_NAME = '__name'
    __ENTITY_PROPERTY_CREDIT_BACK_PERCENT = '__credit_back_percent'
    __ENTITY_PROPERTY_SPENT_AMOUNT_MIN = 'spent_amount_min'
    __ENTITY_PROPERTY_SPENT_AMOUNT_MAX = 'spent_amount_max'
    __ENTITY_PROPERTY_IS_DELETED = '__is_deleted'

    def __init__(self):
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_CUSTOMER_TIERS_TIER'
        self.__reflector = Reflector()

    def save(self, entity: CustomerTier) -> None:
        entity_data = self.__reflector.extract(entity, [
            self.__class__.__ENTITY_PROPERTY_ID,
            self.__class__.__ENTITY_PROPERTY_NAME,
            self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT,
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN,
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX,
            self.__class__.__ENTITY_PROPERTY_IS_DELETED
        ])

        document_id = entity_data[self.__class__.__ENTITY_PROPERTY_ID].value
        document_data = {
            'name': entity_data[self.__class__.__ENTITY_PROPERTY_NAME].value,
            'credit_back_percent': entity_data[self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT].value,
            'spent_amount_min': entity_data[self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN],
            'spent_amount_max': entity_data[self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX],
            'is_deleted': entity_data[self.__class__.__ENTITY_PROPERTY_IS_DELETED],
        }

        # fix of "TypeError: Float types are not supported. Use Decimal types instead." error
        document_data = json.loads(json.dumps(document_data), parse_float=Decimal)

        self.__dynamo_db.put_item(document_id, document_data)

    def __restore(self, row: dict) -> CustomerTier:
        entity = self.__reflector.construct(CustomerTier, {
            self.__class__.__ENTITY_PROPERTY_ID: Id(str(row['sk'])),
            self.__class__.__ENTITY_PROPERTY_NAME: Name(row['name']),
            self.__class__.__ENTITY_PROPERTY_CREDIT_BACK_PERCENT: Percentage(int(row['credit_back_percent'])),
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MIN: int(row['spent_amount_min']),
            self.__class__.__ENTITY_PROPERTY_SPENT_AMOUNT_MAX: int(row['spent_amount_max']),
            self.__class__.__ENTITY_PROPERTY_IS_DELETED: row['is_deleted'],
        })

        return entity

    def get_by_id(self, tier_id: Id) -> Optional[CustomerTier]:
        if not isinstance(tier_id, Id):
            raise ArgumentTypeException(self.get_by_id, 'tier_id', tier_id)

        row = self.__dynamo_db.find_item(tier_id.value)
        return self.__restore(row) if row else None

    def get_all(self) -> Tuple[CustomerTier]:
        rows = self.__dynamo_db.find_all()
        result = [self.__restore(row) for row in rows]
        result = [entity for entity in result if not entity.is_deleted]
        result = tuple(result)
        return result

    def get_neutral(self) -> CustomerTier:
        for tier in self.get_all():
            if tier.is_neutral:
                return tier
        else:
            raise ApplicationLogicException('Neutral Tier does not exist!')


# ----------------------------------------------------------------------------------------------------------------------


class _CustomerDynamoDbStorage(CustomerStorageInterface):
    def __init__(self):
        # @todo : link to tier should be with other data. Will be moved to Nexus soon.
        # """
        # curl -X DELETE localhost:9200/customer_tiers_customer_tiers
        # curl -X PUT localhost:9200/customer_tiers_customer_tiers -H "Content-Type: application/json" -d'{
        #     "mappings": {
        #         "customer_tiers_customer_tiers": {
        #             "properties": {
        #                 "tier_id": {"type": "integer"}
        #             }
        #         }
        #     }
        # }'
        # """
        # self.__tier_elastic = Elastic(
        #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS,
        #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS
        # )
        self.__tiers_dynamo = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__tiers_dynamo.PARTITION_KEY = 'PURCHASE_CUSTOMER_TIERS_CUSTOMER_TIER'

        self.__tiers_storage = CustomerTierStorageImplementation()

    def save(self, customer: CustomerInterface) -> None:
        if not isinstance(customer, CustomerInterface):
            raise ArgumentTypeException(self.save, 'customer', customer)

        information_model = InformationModel(customer.customer_id.value)

        # delete old addresses
        for info_address in information_model.get_information().addresses:
            information_model.delete_address(info_address.address_hash)

        # insert new
        information_model.add_addresses([{
            'address_nickname': delivery_address.address_nickname,
            'recipient_name': delivery_address.recipient_name,
            'mobile_number': delivery_address.phone_number,
            'business_name': delivery_address.business_name,
            'complex_building': delivery_address.complex_building,
            'street_address': delivery_address.street_address,
            'suburb': delivery_address.suburb,
            'postal_code': delivery_address.postal_code,
            'city': delivery_address.city,
            'province': delivery_address.province,
            'special_instructions': delivery_address.special_instructions,
            'business_type': delivery_address.address_type == CustomerInterface.DeliveryAddress.ADDRESS_TYPE_BUSINESS,
        } for delivery_address in customer.delivery_addresses])

        # @todo : link to tier should be with other data. Will be moved to Nexus soon.
        # update tier
        # if self.__tier_elastic.get_data(customer.email.value):
        #     self.__tier_elastic.update_data(customer.email.value, {'doc': {'tier_id': customer.tier.id.value}})
        # else:
        #     self.__tier_elastic.create(customer.email.value, {'tier_id': customer.tier.id.value})
        self.__tiers_dynamo.put_item(customer.email.value, {'tier_id': customer.tier.id.value})

    def get_by_id(self, customer_id: Id) -> Optional[CustomerInterface]:
        if not isinstance(customer_id, Id):
            raise ArgumentTypeException(self.get_by_id, 'customer_id', customer_id)

        information = InformationModel(customer_id.value).get_information()
        if not information.email:
            raise ValueError('User information is incorrect - {}'.format(information.to_dict()))

        # @todo : link to tier should be with other data. Will be moved to Nexus soon.
        # row = self.__tier_elastic.get_data(information.email)
        row = self.__tiers_dynamo.find_item(information.email)

        customer_tier = self.__tiers_storage.get_by_id(Id(str(row['tier_id']))) \
            if row else self.__tiers_storage.get_neutral()
        if not customer_tier:
            # something wrong with tiers set
            raise ValueError('Customer {} is assigned to unknown Customer Tier #{}'.format(
                customer_id.value,
                row['tier_id']
            ))

        return CustomerImplementation(customer_id, information, customer_tier)


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class CustomerStorageImplementation(CustomerStorageInterface):
    def __init__(self):
        self.__storage = _CustomerDynamoDbStorage()

    def save(self, customer: CustomerInterface) -> None:
        return self.__storage.save(customer)

    def get_by_id(self, customer_id: Id) -> Optional[CustomerInterface]:
        return self.__storage.get_by_id(customer_id)


# instead of di-container, factories, etc.
class CustomerTierStorageImplementation(CustomerTierStorageInterface):
    def __init__(self):
        self.__storage = _CustomerTiersStorageDynamoDb()

    def save(self, customer_tier: CustomerTier) -> None:
        self.__storage.save(customer_tier)

    def get_by_id(self, tier_id: Id) -> Optional[CustomerTier]:
        return self.__storage.get_by_id(tier_id)

    def get_all(self) -> Tuple[CustomerTier]:
        return self.__storage.get_all()

    def get_neutral(self) -> CustomerTier:
        return self.__storage.get_neutral()


# ----------------------------------------------------------------------------------------------------------------------

