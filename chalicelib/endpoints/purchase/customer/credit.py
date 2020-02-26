from chalice import Blueprint
from chalicelib.settings import settings
from chalicelib.extensions import *
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.credit.storage import CreditStorageImplementation


def register_customer_credit(blueprint: Blueprint):
    def __get_user():
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user

    @blueprint.route('/customer/credit/info', methods=['GET'], cors=True)
    def customer_credit_info():
        try:
            user = __get_user()

            """"""
            # @TODO : REFACTORING !!!
            from chalicelib.libs.purchase.customer.sqs import FbucksChargeSqsHandler
            see = FbucksChargeSqsHandler
            """"""

            # fbucks amount
            __fbucks_customer_amount_elastic = Elastic(
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT,
            )
            fbucks_amount_row = __fbucks_customer_amount_elastic.get_data(user.id)
            fbucks_amount = fbucks_amount_row['amount'] or 0 if fbucks_amount_row else 0

            # fbucks history
            __fbucks_customer_amount_changes_elastic = Elastic(
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
                settings.AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES,
            )
            fbucks_changes = __fbucks_customer_amount_changes_elastic.post_search({
                "query": {"term": {"customer_id": user.id}}
            }).get('hits', {}).get('hits', []) or []
            fbucks_changes = [fbucks_change['_source'] for fbucks_change in fbucks_changes]

            fbucks_changes = [{
                'amount': fbucks_change['amount'],
                'changed_at': fbucks_change['changed_at'],
                'order_number': fbucks_change['order_number'],
            } for fbucks_change in fbucks_changes]

            # cash out balance
            credit = CreditStorageImplementation().load(user.email)
            cash_out_balance = credit.paid if credit else 0

            return {
                'fbucks_amount': fbucks_amount,
                'fbucks_changes': fbucks_changes,
                'cache_out_balance': cash_out_balance,
            }

        except BaseException as e:
            return http_response_exception_or_throw(e)

