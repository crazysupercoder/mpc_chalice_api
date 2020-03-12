import math
from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.purchase.core import Id
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.models.mpc.Product import Product as MpcProduct
from chalicelib.libs.purchase.checkout.storage import CheckoutStorageImplementation
from chalicelib.libs.purchase.checkout.service import CheckoutAppService
from chalicelib.libs.purchase.order.dtd_calculator import DtdCalculatorImplementation


def register_checkout(blueprint: Blueprint):
    def __get_user() -> User:
        user = blueprint.current_request.current_user
        if user.is_anyonimous:
            raise HttpAuthenticationRequiredException()

        return user

    def __response_checkout(user_id) -> dict:
        products = MpcProduct()
        dtd_calculator = DtdCalculatorImplementation()
        checkout_storage = CheckoutStorageImplementation()

        customer_id = Id(user_id)
        checkout = checkout_storage.load(customer_id)
        if not checkout:
            raise ApplicationLogicException('Checkout is not initiated!')

        tier = blueprint.current_request.current_user.profile.tier

        checkout_items_data = []
        for checkout_item in checkout.checkout_items:
            product = products.getRawDataBySimpleSku(checkout_item.simple_sku.value)
            product_sizes = product.get('sizes', []) if product else tuple()
            size = tuple(filter(lambda s: s.get('simple_sku') == checkout_item.simple_sku.value, product_sizes))[0]
            dtd = dtd_calculator.calculate(checkout_item.simple_sku, checkout_item.qty)

            item_fbucks = None
            if not tier['is_neutral'] and not blueprint.current_request.current_user.is_anyonimous:
                item_fbucks = math.ceil(checkout_item.current_cost.value * tier['discount_rate'] / 100)

            checkout_items_data.append({
                'sku': product['sku'],
                'simple_sku': checkout_item.simple_sku.value,
                'name': product.get('title'),
                'brand_name': product.get('brand'),
                'size_name': size.get('size'),
                'image_url': product.get('image', {}).get('src', None),
                'qty_available': int(size.get('qty')),
                'qty_added': checkout_item.qty.value,
                'is_added_over_limit': checkout_item.is_added_over_limit,
                'product_original_price': checkout_item.product_original_price.value,
                'product_current_price': checkout_item.product_current_price.value,
                'original_cost': checkout_item.original_cost.value,
                'current_cost': checkout_item.current_cost.value,
                'dtd': {
                    'occasion': {
                        'name': dtd.occasion.name.value,
                        'description': dtd.occasion.description.value,
                    } if dtd.occasion else None,
                    'date_from': dtd.date_from.strftime('%Y-%m-%d'),
                    'date_to': dtd.date_to.strftime('%Y-%m-%d'),
                    'working_days_from': dtd.working_days_from,
                    'working_days_to': dtd.working_days_to,
                },
                'fbucks': item_fbucks,
            })

        return {
            'checkout_items': checkout_items_data,
            'original_subtotal': checkout.original_subtotal.value,
            'current_subtotal': checkout.current_subtotal.value,
            'current_subtotal_vat_amount': checkout.current_subtotal_vat_amount.value,
            'delivery_cost': checkout.delivery_cost.value,
            'credits_available': checkout.credits_amount_available.value,
            'credits_in_use': checkout.credits_amount_in_use.value,
            'total_due': checkout.total_due.value,
            'delivery_address': {
                'hash': checkout.delivery_address.address_hash,
                'recipient_name': checkout.delivery_address.recipient_name,
                'phone_number': checkout.delivery_address.phone_number,
                'street_address': checkout.delivery_address.street_address,
                'suburb': checkout.delivery_address.suburb,
                'city': checkout.delivery_address.city,
                'province': checkout.delivery_address.province,
                'complex_building': checkout.delivery_address.complex_building,
                'postal_code': checkout.delivery_address.postal_code,
                'business_name': checkout.delivery_address.business_name,
                'special_instructions': checkout.delivery_address.special_instructions,
            } if checkout.delivery_address else None,
        }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                       VIEW
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/checkout/view', methods=['GET'], cors=True)
    def checkout_view():
        try:
            user = __get_user()
            return __response_checkout(user.id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   INIT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/checkout/init', methods=['POST'], cors=True)
    def checkout_init():
        try:
            user = __get_user()
            checkout_app_service = CheckoutAppService()
            checkout_app_service.init(user.id, user.session_id)
            return __response_checkout(user.id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               SET DELIVERY ADDRESS
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/checkout/set-delivery-address', methods=['PUT'], cors=True)
    def checkout_set_delivery_address():
        try:
            request_data = blueprint.current_request.json_body
            customer_address_hash = str(request_data.get('address_hash', '')).strip()
            if not customer_address_hash:
                raise HttpIncorrectInputDataException('"address_hash" is incorrect!')

            user = __get_user()
            checkout_app_service = CheckoutAppService()
            checkout_app_service.set_delivery_address(user.id, customer_address_hash)
            return __response_checkout(user.id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   CREDIT
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/checkout/credit/usage', methods=['POST', 'DELETE'], cors=True)
    def checkout_credit_spend():
        checkout_storage = CheckoutStorageImplementation()

        try:
            user = __get_user()

            checkout = checkout_storage.load(Id(user.id))
            if not checkout:
                raise ApplicationLogicException('Checkout does not exist!')

            if blueprint.current_request.method == 'POST':
                checkout.use_credits()
            elif blueprint.current_request.method == 'DELETE':
                checkout.unuse_credits()

            checkout_storage.save(checkout)

            return __response_checkout(user.id)
        except BaseException as e:
            return http_response_exception_or_throw(e)

    # ------------------------------------------------------------------------------------------------------------------
    #                                               NEXT TIER INDICATION
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/checkout/indication/next-tier', methods=['GET'], cors=True)
    def checkout_next_tier_indication():
        # @todo : this is a crutch
        # This should not be calculated on mpc side. This should be an api request to somewhere,
        # but this is impossible for now, so we have what we have.

        from chalicelib.settings import settings
        # from chalicelib.libs.core.elastic import Elastic
        from chalicelib.libs.models.mpc.base import DynamoModel
        from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
        from chalicelib.libs.purchase.customer.storage import CustomerTierStorageImplementation

        try:
            user_id = __get_user().id

            customers_storage = CustomerStorageImplementation()
            tiers_storage = CustomerTierStorageImplementation()
            customer = customers_storage.get_by_id(Id(user_id))
            if customer.tier.is_neutral:
                return {
                    'currently_spent': 0,
                    'next_tier': None
                }

            # Spent amount for customer can not exist,
            # if customer spent nothing or sqs is delayed, for example.

            # We can use a tier's minimal amount to return a value close to real.
            # elastic = Elastic(
            #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT,
            #     settings.AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT
            # )
            # row = elastic.get_data(customer.email.value)
            dynamo_db = DynamoModel(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
            dynamo_db.PARTITION_KEY = 'PURCHASE_CUSTOMER_SPENT_AMOUNT'
            row = dynamo_db.find_item(customer.email.value)

            customer_spent_amount = float(row['spent_amount'] or 0) if row else customer.tier.spent_amount_min

            current_tiers = list(tiers_storage.get_all())
            current_tiers.sort(key=lambda tier: tier.spent_amount_min)
            for i in range(0, len(current_tiers) - 1):
                if current_tiers[i].id == customer.tier.id:
                    next_tier = current_tiers[i+1]
                    break
            else:
                next_tier = current_tiers[-1]

            return {
                'currently_spent': customer_spent_amount,
                'next_tier': {
                    'name': next_tier.name.value,
                    'amount_min': next_tier.spent_amount_min,
                }
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)

