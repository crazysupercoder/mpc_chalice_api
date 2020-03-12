from typing import Optional, Tuple
from chalicelib.extensions import *
from .values import Id, Name, Percentage


class CustomerTier(object):
    """
    Attention! Customer Tiers have some crutches.
    So this class should be reviewed and refactored later, when crutches are removed.
    """
    # @todo : refactoring

    def __init__(
        self,
        tier_id: Id,
        name: Name,
        credit_back_percent: Percentage,
        spent_amount_min: int,
        spent_amount_max: int
    ):
        if not isinstance(tier_id, Id):
            raise ArgumentTypeException(self.__init__, 'tier_id', tier_id)

        self.__id = tier_id
        self.__set_name(name)
        self.__set_credit_back_percent(credit_back_percent)
        self.__is_deleted = False

        # these are needed only for crutches and will be deleted somewhen
        self.spent_amount_min = spent_amount_min
        self.spent_amount_max = spent_amount_max

    def __set_name(self, name: Name) -> None:
        if not isinstance(name, Name):
            raise ArgumentTypeException(self.__set_name, 'name', name)

        self.__name = name

    def __set_credit_back_percent(self, percentage: Percentage) -> None:
        if not isinstance(percentage, Percentage):
            raise ArgumentTypeException(self.__init__, 'percentage', percentage)

        self.__credit_back_percent = percentage

    def mark_as_deleted(self) -> None:
        if self.is_deleted:
            raise ApplicationLogicException('Tier "{}" is already Deleted!'.format(self.name))

        if self.is_neutral:
            raise ApplicationLogicException('Neutral Tier cannot be Deleted!')

        self.__is_deleted = True

    @property
    def id(self) -> Id:
        return self.__id

    @property
    def name(self) -> Name:
        return self.__name

    @name.setter
    def name(self, name: Name) -> None:
        self.__set_name(name)

    @property
    def credit_back_percent(self) -> Percentage:
        return self.__credit_back_percent

    @credit_back_percent.setter
    def credit_back_percent(self, percentage: Percentage) -> None:
        self.__set_credit_back_percent(percentage)

    @property
    def is_deleted(self) -> bool:
        return self.__is_deleted

    @property
    def is_neutral(self) -> bool:
        return self.credit_back_percent.value == 0


# ----------------------------------------------------------------------------------------------------------------------


class CustomerTierStorageInterface(object):
    def save(self, customer_tier: CustomerTier) -> None:
        raise NotImplementedError()

    def get_by_id(self, tier_id: Id) -> Optional[CustomerTier]:
        """ Deleted items are NOT IGNORED"""
        raise NotImplementedError()

    def get_all(self) -> Tuple[CustomerTier]:
        """ Deleted items are IGNORED """
        raise NotImplementedError()

    def get_neutral(self) -> CustomerTier:
        """
        :raise ApplicationLogicException: if neutral tier is not found (must always exist)
        """
        raise NotImplementedError()

