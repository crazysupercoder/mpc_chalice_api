import json
import datetime
from typing import Tuple, Optional
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.reflector import Reflector

# ----------------------------------------------------------------------------------------------------------------------


class CreditCard(object):
    def __init__(
        self,
        token: str,
        customer_id: str,
        brand: str,
        number_hidden: str,
        expires: datetime.date,
        holder: Optional[str] = None
    ) -> None:
        if not isinstance(token, str):
            raise ArgumentTypeException(self.__init__, 'token', token)
        elif not len(token.strip()):
            raise ArgumentCannotBeEmptyException(self.__init__, 'token')

        if not isinstance(customer_id, str):
            raise ArgumentTypeException(self.__init__, 'customer_id', customer_id)
        elif not len(customer_id.strip()):
            raise ArgumentCannotBeEmptyException(self.__init__, 'customer_id')

        if not isinstance(brand, str):
            raise ArgumentTypeException(self.__init__, 'brand', brand)
        elif not len(brand.strip()):
            raise ArgumentCannotBeEmptyException(self.__init__, 'brand')

        if not isinstance(number_hidden, str):
            raise ArgumentTypeException(self.__init__, 'number_hidden', number_hidden)
        elif not len(number_hidden.strip()):
            raise ArgumentCannotBeEmptyException(self.__init__, 'number_hidden')

        if not isinstance(expires, datetime.date):
            raise ArgumentTypeException(self.__init__, 'expires', expires)
        elif expires < datetime.date.today():
            raise ApplicationLogicException('Unable to create Expired Credit Card!')

        if holder is not None and not isinstance(holder, str):
            raise ArgumentTypeException(self.__init__, 'holder', holder)
        elif holder is not None and not len(holder.strip()):
            raise ArgumentCannotBeEmptyException(self.__init__, 'holder')

        self.__token = token
        self.__customer_id = customer_id
        self.__brand = brand
        self.__number_hidden = number_hidden
        self.__expires = expires
        self.__holder_name = holder
        self.__is_verified = False
        self.__created_at = datetime.datetime.now()

    @property
    def token(self) -> str:
        return self.__token

    @property
    def customer_id(self) -> str:
        return self.__customer_id

    @property
    def brand(self) -> str:
        return self.__brand

    @property
    def number_hidden(self) -> str:
        return self.__number_hidden

    @property
    def expires(self) -> datetime.date:
        return self.__expires

    @property
    def holder(self) -> Optional[str]:
        return self.__holder_name

    @property
    def created_at(self) -> datetime.datetime:
        return self.__created_at

    @property
    def is_verified(self) -> bool:
        return self.__is_verified

    def make_verified(self) -> None:
        if self.is_verified:
            raise ApplicationLogicException('Card is already Verified!')

        self.__is_verified = True


# ----------------------------------------------------------------------------------------------------------------------


class CreditCardsStorageInterface(object):
    def get_by_token(self, token: str) -> Tuple[CreditCard]:
        raise NotImplementedError()

    def get_all_by_customer(self, customer_id: str) -> Tuple[CreditCard]:
        raise NotImplementedError()

    def save(self, card: CreditCard) -> None:
        raise NotImplementedError()

    def remove(self, card: CreditCard) -> None:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class _CreditCardsElasticStorage(CreditCardsStorageInterface):
    __ENTITY_PROPERTY_TOKEN = '__token'
    __ENTITY_PROPERTY_CUSTOMER_ID = '__customer_id'
    __ENTITY_PROPERTY_BRAND = '__brand'
    __ENTITY_PROPERTY_NUMBER_HIDDEN = '__number_hidden'
    __ENTITY_PROPERTY_EXPIRES = '__expires'
    __ENTITY_PROPERTY_HOLDER_NAME = '__holder_name'
    __ENTITY_PROPERTY_IS_VERIFIED = '__is_verified'
    __ENTITY_PROPERTY_CREATED_AT = '__created_at'

    def __init__(self):
        """
        curl -X DELETE localhost:9200/purchase_customer_credit_cards
        curl -X PUT localhost:9200/purchase_customer_credit_cards -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_customer_credit_cards": {
                    "properties": {
                        "token": {"type": "keyword"},
                        "customer_id": {"type": "keyword"},
                        "brand": {"type": "keyword"},
                        "number_hidden": {"type": "keyword"},
                        "expires": {"type": "keyword"}, //2005 -> 2020/05
                        "holder_name": {"type": "keyword"},
                        "is_verified": {"type": "boolean"},
                        "created_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
                    }
                }
            }
        }'
        curl -X DELETE localhost:9200/purchase_customer_credit_cards_customer_map
        curl -X PUT localhost:9200/purchase_customer_credit_cards_customer_map -H "Content-Type: application/json" -d'{
            "mappings": {
                "purchase_customer_credit_cards_customer_map": {
                    "properties": {
                        "tokens_json": {"type": "keyword"}
                    }
                }
            }
        }'
        """
        self.__elastic_cards = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS,
            settings.AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS
        )
        self.__elastic_customer_cards_map = Elastic(
            settings.AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS_CUSTOMER_MAP,
            settings.AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS_CUSTOMER_MAP
        )
        self.__reflector = Reflector()

    def __restore(self, row: dict) -> CreditCard:
        card = self.__reflector.construct(CreditCard, {
            self.__class__.__ENTITY_PROPERTY_TOKEN: row['token'],
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID: row['customer_id'],
            self.__class__.__ENTITY_PROPERTY_BRAND: row['brand'],
            self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN: row['number_hidden'],
            self.__class__.__ENTITY_PROPERTY_EXPIRES: (
                datetime.date(
                    year=int('20' + row['expires'][0:2]),
                    month=12,
                    day=31
                )
            ) if int(row['expires'][2:4]) == 12 else (
                datetime.date(
                    year=int('20' + row['expires'][0:2]),
                    month=int(row['expires'][2:4]) + 1,
                    day=1
                ) - datetime.timedelta(days=1)
            ),
            self.__class__.__ENTITY_PROPERTY_HOLDER_NAME: row['holder_name'],
            self.__class__.__ENTITY_PROPERTY_IS_VERIFIED: row['is_verified'],
            self.__class__.__ENTITY_PROPERTY_CREATED_AT: datetime.datetime.strptime(
                row['created_at'],
                '%Y-%m-%d %H:%M:%S'
            )
        })

        return card

    def get_by_token(self, token: str) -> Optional[CreditCard]:
        if not isinstance(token, str):
            raise ArgumentTypeException(self.get_by_token, 'token', token)
        elif not token.strip():
            raise ArgumentCannotBeEmptyException(self.get_by_token, 'token')

        row = self.__elastic_cards.get_data(token)
        return self.__restore(row) if row else None

    def get_all_by_customer(self, customer_id: str) -> Tuple[CreditCard]:
        if not isinstance(customer_id, str):
            raise ArgumentTypeException(self.get_all_by_customer, 'customer_id', customer_id)
        elif not customer_id.strip():
            raise ArgumentCannotBeEmptyException(self.get_all_by_customer, 'customer_id')

        customer_cards_map = self.__elastic_customer_cards_map.get_data(customer_id)
        tokens = json.loads(customer_cards_map['tokens_json']) if customer_cards_map else []

        result = [self.get_by_token(token) for token in tokens]
        result = [card for card in result if card]
        if len(result) != len(tokens):
            raise ValueError('Incorrect cards set for customer #{}: existed cards - {}, tokens in map - {}'.format(
                customer_id,
                len(result),
                len(tokens)
            ))

        return tuple(result)

    def save(self, card: CreditCard) -> None:
        if not isinstance(card, CreditCard):
            raise ArgumentTypeException(self.save, 'card', card)

        data = self.__reflector.extract(card, [
            self.__class__.__ENTITY_PROPERTY_TOKEN,
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID,
            self.__class__.__ENTITY_PROPERTY_BRAND,
            self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN,
            self.__class__.__ENTITY_PROPERTY_EXPIRES,
            self.__class__.__ENTITY_PROPERTY_HOLDER_NAME,
            self.__class__.__ENTITY_PROPERTY_IS_VERIFIED,
            self.__class__.__ENTITY_PROPERTY_CREATED_AT,
        ])

        token = data[self.__class__.__ENTITY_PROPERTY_TOKEN]
        customer_id = data[self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID]
        if self.__elastic_cards.get_data(token):
            self.__elastic_cards.update_data(token, {
                'doc': {'is_verified': data[self.__class__.__ENTITY_PROPERTY_IS_VERIFIED]}
            })
        else:
            self.__elastic_cards.create(token, {
                'token': data[self.__class__.__ENTITY_PROPERTY_TOKEN],
                'customer_id': data[self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID],
                'brand': data[self.__class__.__ENTITY_PROPERTY_BRAND],
                'number_hidden': data[self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN],
                'expires': data[self.__class__.__ENTITY_PROPERTY_EXPIRES].strftime('%y%m'),
                'holder_name': data[self.__class__.__ENTITY_PROPERTY_HOLDER_NAME],
                'is_verified': data[self.__class__.__ENTITY_PROPERTY_IS_VERIFIED],
                'created_at': data[self.__class__.__ENTITY_PROPERTY_CREATED_AT].strftime('%Y-%m-%d %H:%M:%S')
            })

            # Elastic can search by attributes only after 1 second from last update.
            # We need all data, when we are searching by customer_id,
            # so in this case we will lost fresh data, if search directly after creation of new card.
            # In this case we need to use another index and get data by elastic doc_id.
            customer_cards_map = self.__elastic_customer_cards_map.get_data(customer_id)
            if customer_cards_map:
                tokens = json.loads(customer_cards_map['tokens_json'])
                tokens.append(token)
                self.__elastic_customer_cards_map.update_data(customer_id, {
                    'doc': {'tokens_json': json.dumps(tokens)}
                })
            else:
                self.__elastic_customer_cards_map.create(customer_id, {
                    'tokens_json': json.dumps([token])
                })

    def remove(self, card: CreditCard) -> None:
        if not isinstance(card, CreditCard):
            raise ArgumentTypeException(self.remove, 'card', card)

        if not self.__elastic_cards.get_data(card.token):
            raise ArgumentValueException('Card #{} is already Removed!'.format(card.token))

        self.__elastic_cards.delete_by_id(card.token)

        customer_cards_map = self.__elastic_customer_cards_map.get_data(card.customer_id)
        tokens = json.loads(customer_cards_map['tokens_json'])
        tokens = [token for token in tokens if token != card.token]
        self.__elastic_customer_cards_map.update_data(card.customer_id, {
            'doc': {'tokens_json': json.dumps(tokens)}
        })


# ----------------------------------------------------------------------------------------------------------------------


class _CreditCardsStorageDynamoDb(CreditCardsStorageInterface):
    __ENTITY_PROPERTY_TOKEN = '__token'
    __ENTITY_PROPERTY_CUSTOMER_ID = '__customer_id'
    __ENTITY_PROPERTY_BRAND = '__brand'
    __ENTITY_PROPERTY_NUMBER_HIDDEN = '__number_hidden'
    __ENTITY_PROPERTY_EXPIRES = '__expires'
    __ENTITY_PROPERTY_HOLDER_NAME = '__holder_name'
    __ENTITY_PROPERTY_IS_VERIFIED = '__is_verified'
    __ENTITY_PROPERTY_CREATED_AT = '__created_at'

    def __init__(self):
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_CUSTOMER_CREDIT_CARDS'
        self.__reflector = Reflector()

    def __restore(self, row: dict) -> CreditCard:
        card = self.__reflector.construct(CreditCard, {
            self.__class__.__ENTITY_PROPERTY_TOKEN: row['sk'],
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID: row['customer_id'],
            self.__class__.__ENTITY_PROPERTY_BRAND: row['brand'],
            self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN: row['number_hidden'],
            self.__class__.__ENTITY_PROPERTY_EXPIRES: (
                datetime.date(
                    year=int('20' + row['expires'][0:2]),
                    month=12,
                    day=31
                )
            ) if int(row['expires'][2:4]) == 12 else (
                datetime.date(
                    year=int('20' + row['expires'][0:2]),
                    month=int(row['expires'][2:4]) + 1,
                    day=1
                ) - datetime.timedelta(days=1)
            ),
            self.__class__.__ENTITY_PROPERTY_HOLDER_NAME: row['holder_name'],
            self.__class__.__ENTITY_PROPERTY_IS_VERIFIED: row['is_verified'],
            self.__class__.__ENTITY_PROPERTY_CREATED_AT: datetime.datetime.strptime(
                row['created_at'],
                '%Y-%m-%d %H:%M:%S'
            )
        })

        return card

    def get_by_token(self, token: str) -> Optional[CreditCard]:
        if not isinstance(token, str):
            raise ArgumentTypeException(self.get_by_token, 'token', token)
        elif not token.strip():
            raise ArgumentCannotBeEmptyException(self.get_by_token, 'token')

        row = self.__dynamo_db.find_item(token)
        return self.__restore(row) if row else None

    def get_all_by_customer(self, customer_id: str) -> Tuple[CreditCard]:
        if not isinstance(customer_id, str):
            raise ArgumentTypeException(self.get_all_by_customer, 'customer_id', customer_id)
        elif not customer_id.strip():
            raise ArgumentCannotBeEmptyException(self.get_all_by_customer, 'customer_id')

        items = self.__dynamo_db.find_by_attribute('customer_id', customer_id)
        result = [self.__restore(item) for item in items]
        return tuple(result)

    def save(self, card: CreditCard) -> None:
        if not isinstance(card, CreditCard):
            raise ArgumentTypeException(self.save, 'card', card)

        data = self.__reflector.extract(card, [
            self.__class__.__ENTITY_PROPERTY_TOKEN,
            self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID,
            self.__class__.__ENTITY_PROPERTY_BRAND,
            self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN,
            self.__class__.__ENTITY_PROPERTY_EXPIRES,
            self.__class__.__ENTITY_PROPERTY_HOLDER_NAME,
            self.__class__.__ENTITY_PROPERTY_IS_VERIFIED,
            self.__class__.__ENTITY_PROPERTY_CREATED_AT,
        ])

        self.__dynamo_db.put_item(data[self.__class__.__ENTITY_PROPERTY_TOKEN], {
            'customer_id': data[self.__class__.__ENTITY_PROPERTY_CUSTOMER_ID],
            'brand': data[self.__class__.__ENTITY_PROPERTY_BRAND],
            'number_hidden': data[self.__class__.__ENTITY_PROPERTY_NUMBER_HIDDEN],
            'expires': data[self.__class__.__ENTITY_PROPERTY_EXPIRES].strftime('%y%m'),
            'holder_name': data[self.__class__.__ENTITY_PROPERTY_HOLDER_NAME],
            'is_verified': data[self.__class__.__ENTITY_PROPERTY_IS_VERIFIED],
            'created_at': data[self.__class__.__ENTITY_PROPERTY_CREATED_AT].strftime('%Y-%m-%d %H:%M:%S')
        })

    def remove(self, card: CreditCard) -> None:
        if not isinstance(card, CreditCard):
            raise ArgumentTypeException(self.remove, 'card', card)

        if not self.__dynamo_db.find_item(card.token):
            raise ArgumentValueException('Card #{} is already Removed!'.format(card.token))

        self.__dynamo_db.delete_item(card.token)


# ----------------------------------------------------------------------------------------------------------------------


class CreditCardsStorageImplementation(CreditCardsStorageInterface):
    def __init__(self):
        self.__storage = _CreditCardsStorageDynamoDb()

    def get_by_token(self, token: str) -> Optional[CreditCard]:
        return self.__storage.get_by_token(token)

    def get_all_by_customer(self, customer_id: str) -> Tuple[CreditCard]:
        return self.__storage.get_all_by_customer(customer_id)

    def save(self, card: CreditCard) -> None:
        self.__storage.save(card)

    def remove(self, card: CreditCard) -> None:
        self.__storage.remove(card)


# ----------------------------------------------------------------------------------------------------------------------

