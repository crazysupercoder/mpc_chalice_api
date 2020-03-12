import uuid
import hashlib
import datetime
from chalice import Blueprint, ChaliceViewError, UnprocessableEntityError
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.purchase.payment_methods.peach.webhooks import WebhooksFlowLog, WebhooksDecryptor
from chalicelib.libs.purchase.payment_methods.peach.payments import MobicredPaymentMethod, CreditCardOrderPaymentMethod
from chalicelib.libs.purchase.core import Order
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.order.sqs import OrderChangeSqsSenderEvent
from chalicelib.libs.purchase.payment_methods.peach.cards import CreditCardsStorageImplementation


def register_payment_methods_peach_weebhoks(blueprint: Blueprint) -> None:
    __SUCCESS_RESULT_CODES = [
        '000.000.000',  # Transaction successfully processed in LIVE system
        '000.100.110',  # Transaction successfully processed in TEST system
    ]

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   ENDPOINT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/payment-methods/peach-payments/webhooks', methods=['POST'], cors=True)
    def peach_payments_webhooks():
        """
        https://peachpayments.docs.oppwa.com/tutorials/webhooks
        """

        flow_id = hashlib.md5(str(uuid.uuid4()).encode('utf-8')).hexdigest()
        webhooks_flow_log = WebhooksFlowLog(flow_id)

        try:
            webhooks_flow_log.write('Start')

            headers_dict = {}
            for header_name in blueprint.current_request.headers:
                headers_dict[header_name] = blueprint.current_request.headers.get(header_name)
            webhooks_flow_log.write('Request: {}'.format({
                'headers': headers_dict,
                'body': blueprint.current_request.json_body
            }))

            webhooks_decryptor = WebhooksDecryptor()
            data = webhooks_decryptor.decrypt(
                blueprint.current_request.headers.get('X-Initialization-Vector'),
                blueprint.current_request.headers.get('X-Authentication-Tag'),
                blueprint.current_request.json_body.get('encryptedBody')
            )

            webhooks_flow_log.write('Webhook type: {}'.format(data['type']))

            handlers_map = {
                'PAYMENT': __handle_payment,
                'REGISTRATION': __handle_registration,
                'RISK': __handle_risk,
            }

            if not handlers_map.get(data['type']):
                raise UnprocessableEntityError(
                    'Webhooks handler does not know, how to work with "{}" webhooks type'.format(data['type'])
                )

            handlers_map[data['type']](data, webhooks_flow_log)

            webhooks_flow_log.write('End - OK')
        except ChaliceViewError as e:
            webhooks_flow_log.write('End - HTTP Error: {}'.format(str(e)))
            raise e
        except BaseException as e:
            webhooks_flow_log.write('End - Server Error: {}'.format(str(e)))
            raise e

        return 'OK'

    # ------------------------------------------------------------------------------------------------------------------
    #                                               HANDLE PAYMENT
    # ------------------------------------------------------------------------------------------------------------------

    def __handle_payment(data: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        """
        This type of notification is sent when a payment is created or updated in the system.
        """
        payload = data['payload']

        order_storage = OrderStorageImplementation()
        sqs_sender = SqsSenderImplementation()

        order_number = Order.Number(payload['customParameters']['order_number'])
        order = order_storage.load(order_number)
        if not order:
            raise ValueError('Order #{} does not exist!'.format(order_number.value))

        payment_brand = payload['paymentBrand']
        webhooks_flow_log.write('Payment Brand: {}'.format(payment_brand))

        order_payment_method = None
        if payment_brand == 'MOBICRED':
            order_payment_method = MobicredPaymentMethod(payload['id'])
        elif payment_brand != 'MOBICRED':
            order_payment_method = CreditCardOrderPaymentMethod(payload['id'])

        if not order_payment_method:
            raise ValueError('{} does not know, how to work with "{}" payment brand!'.format(
                __handle_payment.__qualname__,
                payment_brand
            ))

        if payload['result']['code'] not in __SUCCESS_RESULT_CODES:
            webhooks_flow_log.write('Skipped because of not good result - {}!'.format(payload['result']))
            return

        # Attention!
        # @TODO : SHOULD BE SPENT, WHEN ORDER IS CREATED !!!
        # Currently we use f-bucks only! Other credits are not available for now!
        # @todo : other credit types
        # @todo : copy-paste code
        # @todo : when reservation of credits amount will be done, perhaps, use sqs to spend credits
        if order.credit_spent_amount.value > 0:
            webhooks_flow_log.write('Spending Credits...')

            """"""
            from chalicelib.libs.purchase.core import Checkout
            see = Checkout.__init__
            """"""
            # @TODO : refactoring : raw data usage
            fbucks_customer_amount_elastic = Elastic(
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
            )
            fbucks_customer_amount_changes_elastic = Elastic(
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
            )
            fbucks_customer_amount_elastic.update_data(order.customer_id.value, {
                'script': 'ctx._source.amount -= ' + str(order.credit_spent_amount.value)
            })
            fbucks_customer_amount_changes_elastic.create(str(uuid.uuid4()) + str(order.customer_id.value), {
                "customer_id": order.customer_id.value,
                "amount": -order.credit_spent_amount.value,
                "changed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "order_number": order.number.value,
            })
            webhooks_flow_log.write('Done')

        # update order
        webhooks_flow_log.write('Updating Order...')
        order.payment_method = order_payment_method
        order.status = Order.Status(Order.Status.PAYMENT_SENT)
        order.status = Order.Status(Order.Status.PAYMENT_RECEIVED)
        order_storage.save(order)
        webhooks_flow_log.write('Done')

        # send sqs
        webhooks_flow_log.write('Sending Order to SQS...')
        sqs_sender.send(OrderChangeSqsSenderEvent(order))
        webhooks_flow_log.write('Done')

    # ------------------------------------------------------------------------------------------------------------------
    #                                               HANDLE REGISTRATION
    # ------------------------------------------------------------------------------------------------------------------

    def __handle_registration(data: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        """
        This type of notification is sent when a registration is created or deleted.
        """
        # looks like, this is only for cards
        # https://peachpayments.docs.oppwa.com/tutorials/server-to-server/tokenisation

        # Indicator of status change. This field is available only if the type is REGISTRATION.
        action = data['action']
        actions_map = {
            'CREATED': __handle_registration_created,
            'UPDATED': __handle_registration_updated,
            'DELETED': __handle_registration_deleted,
        }
        if action not in actions_map.keys():
            raise Exception('{} does not know how to work with "{}" action!'.format(
                __handle_registration.__qualname__,
                action
            ))

        webhooks_flow_log.write('Action - {}!'.format(action))

        if data['payload']['result']['code'] not in __SUCCESS_RESULT_CODES:
            webhooks_flow_log.write('Skipped because of not good result - {}!'.format(data['payload']['result']))
            return

        actions_map[action](data['payload'], webhooks_flow_log)

    def __handle_registration_created(payload: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        cards_storage = CreditCardsStorageImplementation()
        card = cards_storage.get_by_token(payload['id'])
        if not card:
            raise ValueError('Credit Card #{} does not exist!'.format(payload['id']))

        webhooks_flow_log.write('Card verification...')
        card.make_verified()
        cards_storage.save(card)
        webhooks_flow_log.write('Card verified!')

    def __handle_registration_updated(payload: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        webhooks_flow_log.write('nothing to do here')

    def __handle_registration_deleted(payload: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        webhooks_flow_log.write('nothing to do here')

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   RISK
    # ------------------------------------------------------------------------------------------------------------------

    def __handle_risk(data: dict, webhooks_flow_log: WebhooksFlowLog) -> None:
        """
        This type of notification is sent when a risk transaction is created or deleted.
        """
        webhooks_flow_log.write('is not developed, and, perhaps, not needed to be.')

    # ------------------------------------------------------------------------------------------------------------------


