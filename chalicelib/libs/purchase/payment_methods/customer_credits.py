from chalicelib.libs.purchase.core import Order


class CustomerCreditsOrderPaymentMethod(Order.PaymentMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'customer_credit'

    @classmethod
    def _get_label(cls):
        return 'Customer Credits'

    @property
    def extra_data(self) -> dict:
        return {}
