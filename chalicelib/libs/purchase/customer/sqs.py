import uuid
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.models.mpc.base import DynamoModel
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface
from chalicelib.utils.sqs_handlers.base import SqsMessage, SqsHandlerInterface
from chalicelib.libs.purchase.core import Id, Name, Percentage, CustomerTier
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.purchase.customer.storage import CustomerTierStorageImplementation
from chalicelib.libs.message.base import Message, MessageStorageImplementation
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation


class CustomerTiersTiersSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__tiers_storage = CustomerTierStorageImplementation()

    def handle(self, sqs_message: SqsMessage) -> None:
        incoming_tiers = tuple([CustomerTier(
            Id(str(row.get('id'))),
            Name(str(row.get('name'))),
            Percentage(int(row.get('credit_back_percent'))),
            int(row.get('spent_amount_min')),
            int(row.get('spent_amount_max')),
        ) for row in (sqs_message.message_data.get('tiers'))])

        stored_tiers = self.__tiers_storage.get_all()

        # delete
        incoming_tiers_ids = [incoming_tier.id.value for incoming_tier in incoming_tiers]
        for stored_tier in stored_tiers:
            if stored_tier.id.value not in incoming_tiers_ids:
                stored_tier.mark_as_deleted()
                self.__tiers_storage.save(stored_tier)

        # add / update
        for incoming_tier in incoming_tiers:
            self.__tiers_storage.save(incoming_tier)


# ----------------------------------------------------------------------------------------------------------------------


class CustomerTiersCustomersSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__tiers_storage = CustomerTierStorageImplementation()
        self.__messages = MessageStorageImplementation()
        self.__logger = Logger()

        """"""
        # @todo : refactoring
        from chalicelib.libs.purchase.core import CustomerInterface
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        see = CustomerInterface.tier
        see = CustomerStorageImplementation.save
        """"""
        self.__elastic = Elastic(
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS,
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS
        )

    def handle(self, sqs_message: SqsMessage) -> None:
        # 'tiers' here are the same tiers set as in 'customer_tiers_set' sqs-message.
        # Theoretically this message can be handled earlier than 'customer_tiers_set' message,
        # so we need to be sure, that all new tiers exist.
        incoming_tiers_ids = [row['id'] for row in sqs_message.message_data['tiers']]
        stored_tiers_ids = [tier.id.value for tier in self.__tiers_storage.get_all()]
        if sum([tier_id for tier_id in incoming_tiers_ids if tier_id not in stored_tiers_ids]) > 0:
            # @todo : this is a crutch
            CustomerTiersTiersSqsHandler().handle(SqsMessage(
                sqs_message.id,
                'customer_tiers_set',
                {'tiers': sqs_message.message_data['tiers']}
            ))

        # assign customers to tiers
        tiers = self.__tiers_storage.get_all()
        tiers_map = {}
        for tier in tiers:
            tiers_map[tier.id.value] = tier

        for customer_tier_data in sqs_message.message_data.get('customers'):
            customer_email = str(customer_tier_data['email'])
            tier_id = int(customer_tier_data['tier_id'])

            if self.__elastic.get_data(customer_email):
                self.__elastic.update_data(customer_email, {'doc': {
                    'tier_id': tier_id
                }})
            else:
                self.__elastic.create(customer_email, {'tier_id': tier_id})

            # notify user (silently)
            try:
                tier = tiers_map[str(tier_id)]
                self.__messages.save(Message(
                    str(uuid.uuid4()),
                    customer_email,
                    'Your Customer Tier has been changed!',
                    'Now you are in the "{}" Customer Tier!'.format(tier.name.value)
                ))
            except BaseException as e:
                self.__logger.log_exception(e)


# ----------------------------------------------------------------------------------------------------------------------


class FbucksChargeSqsHandler(SqsHandlerInterface):
    # @TODO : REFACTORING !!! currently we are working with raw data

    def __init__(self):
        self.__orders_storage = OrderStorageImplementation()
        self.__logger = Logger()

        # """
        # curl -X DELETE localhost:9200/fbucks_handled_orders
        # curl -X PUT localhost:9200/fbucks_handled_orders -H "Content-Type: application/json" -d'{
        #     "mappings": {
        #         "fbucks_handled_orders": {
        #             "properties": {
        #                 "handled_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
        #             }
        #         }
        #     }
        # }'
        # """
        # self.__fbucks_handled_orders_elastic = Elastic(
        #     settings.AWS_ELASTICSEARCH_FBUCKS_HANDLED_ORDERS,
        #     settings.AWS_ELASTICSEARCH_FBUCKS_HANDLED_ORDERS,
        # )
        self.__fbucks_handled_orders_dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__fbucks_handled_orders_dynamo_db.PARTITION_KEY = 'PURCHASE_FBUCKS_REWARD_HANDLED_ORDERS'


        # Attention!
        # We can get current customer's amount as a sum of all changes by customer_id
        # But theoretically elastic can not be in time with index update (1 second) between requests.
        # So there is another index to store amount value.
        """
        curl -X DELETE localhost:9200/fbucks_customer_amount
        curl -X PUT localhost:9200/fbucks_customer_amount -H "Content-Type: application/json" -d'{
            "mappings": {
                "fbucks_customer_amount": {
                    "properties": {
                        "amount": {"type": "integer"}
                    }
                }
            }
        }'
        curl -X DELETE localhost:9200/fbucks_customer_amount_changes
        curl -X PUT localhost:9200/fbucks_customer_amount_changes -H "Content-Type: application/json" -d'{
            "mappings": {
                "fbucks_customer_amount_changes": {
                    "properties": {
                        "customer_id": {"type": "keyword"},
                        "amount": {"type": "integer"},
                        "changed_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "order_number": {"type": "keyword"}
                    }
                }
            }
        }'
        """
        self.__fbucks_customer_amount_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
        )
        self.__fbucks_customer_amount_changes_elastic = Elastic(
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
            settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
        )

        self.__customer_storage = CustomerStorageImplementation()
        self.__messages_storage = MessageStorageImplementation()

    def handle(self, sqs_message: SqsMessage) -> None:
        import uuid
        import datetime
        from chalicelib.libs.purchase.core import Order

        order_number_values = sqs_message.message_data['order_numbers']
        for order_number_value in order_number_values:
            try:
                now_string = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # skip duplicates
                # if self.__fbucks_handled_orders_elastic.get_data(order_number_value):
                if self.__fbucks_handled_orders_dynamo_db.find_item(order_number_value):
                    self.__logger.log_simple('{}: Fbucks for order #{} already earned!'.format(
                        self.handle.__qualname__,
                        order_number_value
                    ))
                    continue

                # ignore orders without fbucks amounts
                order = self.__orders_storage.load(Order.Number(order_number_value))
                fbucks_amount = order.total_fbucks_earnings.value
                if fbucks_amount == 0:
                    # remember order as handled
                    # self.__fbucks_handled_orders_elastic.create(order_number_value, {'handled_at': now_string})
                    self.__fbucks_handled_orders_dynamo_db.put_item(order_number_value, {'handled_at': now_string})
                    continue

                # earn fbucks
                self.__fbucks_customer_amount_elastic.update_data(order.customer_id.value, {
                    'script': 'ctx._source.amount += ' + str(fbucks_amount),
                    'upsert': {
                        'amount': fbucks_amount,
                    }
                })
                self.__fbucks_customer_amount_changes_elastic.create(str(uuid.uuid4()) + str(order.customer_id.value), {
                    "customer_id": order.customer_id.value,
                    "amount": +fbucks_amount,
                    "changed_at": now_string,
                    "order_number": order_number_value,
                })

                # remember order as handled
                # self.__fbucks_handled_orders_elastic.create(order_number_value, {'handled_at': now_string})
                self.__fbucks_handled_orders_dynamo_db.put_item(order_number_value, {'handled_at': now_string})

                # notify (silently)
                try:
                    customer = self.__customer_storage.get_by_id(order.customer_id)
                    self.__messages_storage.save(Message(
                        str(uuid.uuid4()),
                        customer.email.value,
                        'F-Bucks has been Earned!',
                        'You have earned {} F-Bucks by your Order #{}'.format(
                            fbucks_amount,
                            order.number.value
                        )
                    ))
                except BaseException as e:
                    self.__logger.log_exception(e)

            except BaseException as e:
                self.__logger.log_exception(e)


# ----------------------------------------------------------------------------------------------------------------------


class CrutchCustomerSpentAmountSqsHandler(SqsHandlerInterface):
    """
    @todo : this is a crutch
    This should be an api request, but it is impossible for now,
    so we use sqs-communication.
    This data is used to calculate next possible tier on mpc side.
    """

    def __init__(self):
        # """
        # curl -X DELETE localhost:9200/customer_tiers_customer_info_spent_amount
        # curl -X PUT localhost:9200/customer_tiers_customer_info_spent_amount -H "Content-Type: application/json" -d'{
        #     "mappings": {
        #         "customer_tiers_customer_info_spent_amount": {
        #             "properties": {
        #                 "spent_amount": {"type": "float"}
        #             }
        #         }
        #     }
        # }'
        # """
        # self.__elastic = Elastic(
        #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT,
        #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT
        # )
        self.__dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
        self.__dynamo_db.PARTITION_KEY = 'PURCHASE_CUSTOMER_SPENT_AMOUNT'

    def handle(self, sqs_message: SqsMessage) -> None:
        for item in sqs_message.message_data.get('items'):
            customer_email = str(item['customer_email'])
            spent_amount = int(item['spent_amount'])

            # if self.__elastic.get_data(customer_email):
            #     self.__elastic.update_data(customer_email, {'doc': {'spent_amount': spent_amount}})
            # else:
            #     self.__elastic.create(customer_email, {'spent_amount': spent_amount})
            self.__dynamo_db.put_item(customer_email, {'spent_amount': spent_amount})

# ----------------------------------------------------------------------------------------------------------------------


class CrutchCustomerInfoRequestSqsSenderEvent(SqsSenderEventInterface):
    """
    @todo : this is a crutch
    This should be an api request, but it is impossible for now,
    so we use sqs-communication.
    """

    def __init__(self, email: str):
        self.__email = email

    @classmethod
    def _get_event_type(cls) -> str:
        return 'customer_info_request'

    @property
    def event_data(self) -> dict:
        return {
            'customer_email': self.__email
        }


class CrutchCustomerInfoRequestAnswerSqsHandler(SqsHandlerInterface):
    """
    @todo : this is a crutch
    This should be an api request, but it is impossible for now,
    so we use sqs-communication.
    """
    def __init__(self):
        self.__tiers_storage = CustomerTierStorageImplementation()
        self.__messages = MessageStorageImplementation()
        self.__logger = Logger()

    def handle(self, sqs_message: SqsMessage) -> None:
        """
        crutch
        customer_info_request_answer
        [
            'customer_email' => $customer->getEmail(),
            'name' => [
                'first' => $customer->getFirstName() ?: null,
                'last' => $customer->getLastName() ?: null,
            ],
            'gender' => 'F' / 'M',
            'addresses' => [
                [
                    'nickname' => $address->getAddressNickname() ?: null,
                    'phone' => $address->getTelephone() ?: $address->getContactNumber() ?: null,
                    'street' => $address->getStreet() ?: null,
                    'suburb' => $address->getSuburb() ?: null,
                    'post_code' => $address->getPostCode() ?: null,
                    'city' => $address->getCity() ?: null,
                    'province' => $address->getProvince() ?: null,
                    'country_code' => $address->getCountryCode() ?: null,
                    'is_default_billing' => $address->getId() == $customer->getDefaultBillingAddressId(),
                    'is_default_shipping' => $address->getId() == $customer->getDefaultShippingAddressId(),
                ],
                ...
            ],
            'tier' => [
                'id' => $customerTier->getId(),
                'name' => $customerTier->getName(),
                'credit_back_percent' => $customerTier->getCreditBackPercent(),
                'spent_amount_min' => $customerTier->getSpendAmountMin(),
                'spent_amount_max' => $customerTier->getSpendAmountMax(),
            ]
        ]
        """

        # @todo : perhaps, update other data - needs to be able to get customer by email

        # 'tier' here is the same tier as in 'customer_tiers_set' sqs-message.
        # Theoretically this message can be handled earlier than 'customer_tiers_set' message,
        # so we need to be sure, that all new tiers exist.
        tier_data = sqs_message.message_data['tier']
        tier = self.__tiers_storage.get_by_id(Id(str(tier_data['id'])))
        if not tier:
            tier = CustomerTier(
                Id(str(tier_data['id'])),
                Name(tier_data['name']),
                Percentage(int(tier_data['credit_back_percent'])),
                int(tier_data['spent_amount_min']),
                int(tier_data['spent_amount_max'])
            )
            self.__tiers_storage.save(tier)

        # assign user to tier
        """"""
        # @todo : refactoring
        from chalicelib.libs.purchase.core import CustomerInterface
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        see = CustomerInterface.tier
        see = CustomerStorageImplementation.save
        """"""
        customer_email = sqs_message.message_data['customer_email']
        elastic = Elastic(
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS,
            settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS
        )
        if elastic.get_data(customer_email):
            elastic.update_data(customer_email, {'doc': {'tier_id': tier.id.value}})
        else:
            elastic.create(customer_email, {'tier_id': tier.id.value})


# ----------------------------------------------------------------------------------------------------------------------

