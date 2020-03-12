from chalicelib.libs.purchase.core import Order


class RegularEftOrderPaymentMethod(Order.PaymentMethodAbstract):
    @classmethod
    def _get_descriptor(cls):
        return 'regular_eft'

    @classmethod
    def _get_label(cls):
        return 'Regular EFT'

    @property
    def extra_data(self) -> dict:
        return {}

