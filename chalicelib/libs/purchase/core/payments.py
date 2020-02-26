

class PaymentMethodAbstract(object):
    @classmethod
    def _get_descriptor(cls):
        raise NotImplementedError()

    @classmethod
    def _get_label(cls):
        raise NotImplementedError()

    @property
    def descriptor(self) -> str:
        return self.__class__._get_descriptor()

    @property
    def label(self) -> str:
        return self.__class__._get_label()

    @property
    def extra_data(self) -> dict:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class RefundMethodAbstract(object):
    @classmethod
    def _get_descriptor(cls):
        raise NotImplementedError()

    @classmethod
    def _get_label(cls):
        raise NotImplementedError()

    @property
    def descriptor(self) -> str:
        return self.__class__._get_descriptor()

    @property
    def label(self) -> str:
        return self.__class__._get_label()

    @property
    def extra_data(self) -> dict:
        raise NotImplementedError()

