import datetime
from chalicelib.extensions import *
from chalice import Blueprint
from chalicelib.settings import Config
from chalicelib.libs.purchase.core import Order, CustomerInterface
from chalicelib.libs.purchase.order.storage import OrderStorageImplementation
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation
from chalicelib.libs.models.mpc.Product import Product as MpcProducts


def register_orders(blueprint: Blueprint) -> None:
    def __to_utc(local_datetime: datetime.datetime) -> datetime.datetime:
        return local_datetime.astimezone(tz=datetime.timezone.utc)

    # ------------------------------------------------------------------------------------------------------------------

    def __check_header_or_error():
        config = Config()
        read_api_header_value = blueprint.current_request.headers.get(config.READ_API_HEADER_NAME)
        if read_api_header_value != config.READ_API_HEADER_VALUE:
            raise HttpIncorrectInputDataException('Authentication is missed!')

    # ------------------------------------------------------------------------------------------------------------------

    def _order_info(order: Order) -> dict:
        customer_storage = CustomerStorageImplementation()
        mpc_products = MpcProducts()

        customer = customer_storage.get_by_id(order.customer_id)
        delivery_address = order.delivery_address

        order_items_data = []
        for order_item in order.items:
            product = mpc_products.getRawDataBySimpleSku(order_item.simple_sku.value, False)
            size = tuple(filter(lambda s: s['rs_simple_sku'] == order_item.simple_sku.value, product['sizes']))[0]

            simple_id = int(size['portal_simple_id'])

            qty_ordered = order_item.qty_ordered.value
            qty_canceled_before_payment = order_item.qty_cancelled_before_payment.value
            qty_canceled_after_payment = order_item.qty_cancelled_after_payment_cancelled.value
            qty_invoiced = (
                qty_ordered
                - qty_canceled_before_payment
            ) if order.was_paid else 0
            qty_shipped = (
                qty_ordered
                - qty_canceled_before_payment
                - qty_canceled_after_payment
            ) if order.was_delivered else 0
            qty_returned = order_item.qty_return_returned.value
            qty_refunded = order_item.qty_refunded.value

            # @todo : not done yet
            base_discount_amount = 0

            # 1 - may mean "usual item", then 2 - "promo item", ...
            items_group_id = str(int('1%010d' % simple_id))

            order_items_data.append({
                'DTD': str(order_item.dtd.working_days_from) + '-' + str(order_item.dtd.working_days_to),
                'item_id': items_group_id,
                'sku': order_item.simple_sku.value,
                'rs_event_code': order_item.event_code.value,

                'qty_ordered': qty_ordered,
                'qty_canceled_before_payment': qty_canceled_before_payment,
                'qty_invoiced': qty_invoiced,
                'qty_canceled_after_payment': qty_canceled_after_payment,
                'qty_shipped': qty_shipped,
                'qty_returned': qty_returned,
                'qty_refunded': qty_refunded,

                'original_rsp': order_item.total_original_cost_ordered.value,
                'original_price': order_item.total_current_cost_ordered.value,

                'base_cost': order_item.product_current_price.value,
                'base_discount_amount': base_discount_amount,
                'base_price_incl_tax': order_item.product_current_price.value,
                'base_tax_amount': (
                    order_item.total_current_cost_ordered.value / (100 + order.vat_percent.value)
                ) * order.vat_percent.value,
                'tax_percent': order.vat_percent.value,
                'base_row_total_incl_tax': order_item.product_current_price.value * order_item.qty_ordered.value,

                'item_selling_incl_marketing_discount': order_item.product_current_price.value,

                'order_tax_amount': order.subtotal_vat_amount.value,
                'order_total_inc_voucher_discounts': order.total_current_cost_ordered.value,

                'rs_store_type': 'MPC',
                'product_id': simple_id,
                'in_time_for_what': '',
                'in_time_for_status': 0,
            })

        if customer.name:
            customer_first_name = customer.name.first_name.value
            customer_last_name = customer.name.last_name.value
        else:
            customer_name_str = delivery_address.recipient_name
            customer_first_name = customer_name_str.split(' ')[0]
            customer_last_name = customer_name_str.replace(customer_first_name, '', 1).strip()

        customer_genders_map = {
            CustomerInterface.Gender.MALE: 'male',
            CustomerInterface.Gender.FEMALE: 'female',
        }

        data = {
            'increment_id': order.number.value,
            'status': order.status.value,
            'created_at': __to_utc(order.created_at).strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': __to_utc(order.updated_at).strftime('%Y-%m-%d %H:%M:%S'),
            'payment': {
                'entity_id': order.number.value,
                'method': order.payment_method.descriptor,
                'additional_information': order.payment_method.extra_data,
                'base_amount_paid': order.total_current_cost_paid.value - order.credit_spent_amount.value,
                'base_amount_refunded': order.total_refunded_cost.value,
            } if order.payment_method else {
                'base_amount_ordered': order.total_current_cost_ordered.value if order.was_paid else 0,
            },
            'status_history': [{
                'status': status_change.status.value,
                'created_at': __to_utc(status_change.datetime).strftime('%Y-%m-%d %H:%M:%S'),
            } for status_change in order.status_history],

            'invoice': [{
                'entity_id': order.number.value,
                'state': 3 if order.was_closed or order.was_cancelled else 2 if order.was_paid else 1,
                'base_grand_total': order.total_current_cost_paid.value - order.credit_spent_amount.value,
                'base_tax_amount': (
                    (
                        order.total_current_cost_paid.value
                        - order.credit_spent_amount.value
                    ) / (100 + order.vat_percent.value)
                ) * order.vat_percent.value,

                # @todo : not developed yet
                'base_discount_amount': 0,

                'base_subtotal_incl_tax': sum([
                    item.product_current_price.value * (
                        (item.qty_ordered.value - item.qty_cancelled_before_payment.value) if order.was_paid else 0
                    )
                    for item in order.items
                ]),
                'base_shipping_amount': order.delivery_cost.value,
                'base_customer_balance_amount': order.credit_spent_amount.value,
            }] if order.was_paid else [],

            'customer_firstname': customer_first_name,
            'customer_lastname': customer_last_name,
            'customer_email': customer.email.value,
            'customer_gender': customer_genders_map.get(customer.gender.descriptor, None) if customer.gender else None,
            'ip_address': None,  # @TODO : add IP address. Not sure, that it should be inside Order. Create Elastic?

            'addresses': [{
                # magento address id. We use only one, so can set order number.
                'entity_id': order.number.value,
                # @todo : we have only one address in order, so let it be 'shipping'
                'address_type': 'shipping',

                'country': 'South Africa',
                'country_id': 'ZA',
                'region': delivery_address.province,
                'city': delivery_address.city,
                'street': delivery_address.street_address,
                'suburb': delivery_address.suburb,
                'postcode': delivery_address.postal_code,
                'company': delivery_address.business_name,
                'firstname': delivery_address.recipient_name.split(' ')[0],
                'lastname': (lambda s: s.replace(s.split(' ')[0] + ' ', '', 1))(delivery_address.recipient_name),
                'telephone': delivery_address.phone_number,
            }],

            'items': order_items_data,

            'base_subtotal_incl_tax': sum([
                item.product_current_price.value * item.qty_ordered.value
                for item in order.items
            ]),
            'base_customer_balance_invoiced': order.credit_spent_amount.value,
        }

        return data

    # ------------------------------------------------------------------------------------------------------------------
    #                                               ORDER INFO
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/orders/info/{order_number}', methods=['POST'], cors=True)
    def info(order_number):
        try:
            __check_header_or_error()

            order_storage = OrderStorageImplementation()
            order = order_storage.load(Order.Number(order_number))
            if not order:
                raise HttpNotFoundException('Order does not exist!')

            return {
                'order': _order_info(order)
            }
        except BaseException as e:
            return http_response_exception_or_throw(e)



