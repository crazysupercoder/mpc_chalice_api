from chalicelib.libs.purchase.core import ReturnRequest


class HandDeliveryMethod(ReturnRequest.DeliveryMethod):
    @classmethod
    def _get_descriptor(cls):
        return 'hand_delivery'

    @classmethod
    def _get_label(cls):
        return 'Hand Delivery'


class CourierOrPostOffice(ReturnRequest.DeliveryMethod):
    @classmethod
    def _get_descriptor(cls):
        return 'courier_or_post_office'

    @classmethod
    def _get_label(cls):
        return 'Courier or Post Office'


class RunwaysaleToCollect(ReturnRequest.DeliveryMethod):
    @classmethod
    def _get_descriptor(cls):
        return 'runwaysale_to_collect'

    @classmethod
    def _get_label(cls):
        return 'RunwaySale to Collect'

