import uuid
from datetime import datetime
from warnings import warn
from typing import Optional, List, Union
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.elastic import Elastic
from chalicelib.libs.core.data_lake import DataLakeBase
from chalicelib.libs.core.datetime import get_mpc_datetime_now
from ..mpc.Cms.weight import WeightModel


class ACTION_TYPE:
    view = 'view'
    click = 'click'
    visit = 'visit'


class _BaseAction(object):
    def __init__(
        self,
        raw_product_data: dict,
        session_id: str,
        user_id: Optional[str] = None,
        user_tier: Optional[dict] = None,
        weight_version: int = None,
        question_score: float = None,
        question_weight: float = None,
        order_score: float = None,
        order_weight: float = None,
        tracking_score: float = None,
        tracking_weight: float = None,
        percentage_score: float = -1,
    ):
        if not isinstance(raw_product_data, dict):
            raise ArgumentTypeException(self.__init__, 'raw_product_data', raw_product_data)
        elif not raw_product_data:
            raise ArgumentCannotBeEmptyException(self.__init__, 'raw_product_data')

        if not isinstance(session_id, str):
            raise ArgumentTypeException(self.__init__, 'session_id', session_id)
        elif not str(session_id).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'session_id')

        if user_id is not None and not isinstance(user_id, str):
            raise ArgumentTypeException(self.__init__, 'user_id', user_id)
        elif user_id is not None and not str(user_id).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'user_id')

        if user_tier is not None and not isinstance(user_tier, dict):
            raise ArgumentTypeException(self.__init__, 'user_tier', user_tier)

        self.__session_id = str(session_id).strip()
        self.__user_id = str(user_id).strip() if user_id else None
        self.__user_tier = user_tier if user_id else None
        self.__raw_product_data = raw_product_data
        self.__is_sold_out = (sum([int(size.get('qty') or 0) for size in raw_product_data.get('sizes', [])]) == 0)
        self.__created_at = get_mpc_datetime_now()
        self.__version = weight_version
        self.__question_score = question_score
        self.__question_weight = question_weight
        self.__order_score = order_score
        self.__order_weight = order_weight
        self.__tracking_score = tracking_score
        self.__tracking_weight = tracking_weight
        self.__percentage_score = percentage_score

    @property
    def session_id(self) -> str:
        return self.__session_id

    @property
    def user_id(self) -> Optional[str]:
        return self.__user_id

    @property
    def user_tier(self) -> Optional[dict]:
        return clone(self.__user_tier)

    @property
    def raw_product_data(self) -> dict:
        return clone(self.__raw_product_data)

    @property
    def config_sku(self) -> str:
        return self.__raw_product_data.get('rs_sku')

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @property
    def version(self) -> int:
        return self.__version

    @property
    def question_score(self) -> float:
        return self.__question_score

    @property
    def question_weight(self) -> float:
        return self.__question_weight

    @property
    def order_score(self) -> float:
        return self.__order_score

    @property
    def order_weight(self) -> float:
        return self.__order_weight

    @property
    def tracking_score(self) -> float:
        return self.__tracking_score

    @property
    def tracking_weight(self) -> float:
        return self.__tracking_weight

    @property
    def percentage_score(self) -> float:
        return self.__percentage_score

    @property
    def is_sold_out(self) -> bool:
        return self.__is_sold_out

    @property
    def total_score(self) -> float:
        try:
            value = sum([
                float(self.question_score) * float(self.question_weight),
                float(self.order_score) * float(self.order_weight),
                float(self.tracking_score) * float(self.tracking_weight),
            ])
            return float("%.2f" % value)
        except Exception as e:
            print(str(e))
            return None

    @property
    def scores(self) -> dict:
        return {
            'version': self.version,
            'qs': self.question_score,
            'qw': self.question_weight,
            'rs': self.order_score,
            'rw': self.order_weight,
            'ts': self.tracking_score,
            'tw': self.tracking_weight,
            'total_score': self.total_score,
            'percentage_score': self.percentage_score,
        }

    def to_dict(self) -> dict:
        return {
            'config_sku': self.config_sku,
            'session_id': self.session_id,
            'user_id': self.user_id,
            'user_tier': self.user_tier,
            'action_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'version': self.version,
            'question_score': self.question_score,
            'question_weight': self.question_weight,
            'order_score': self.order_score,
            'order_weight': self.order_weight,
            'tracking_score': self.tracking_score,
            'tracking_weight': self.tracking_weight,
            'product_score': self.total_score,
            'percentage_score': self.percentage_score,
            'is_sold_out': self.is_sold_out,
        }

    @property
    def action_data(self) -> dict:
        return {
            'version': self.version,
            'qs': self.question_score,
            'qw': self.question_weight,
            'rs': self.order_score,
            'rw': self.order_weight,
            'ts': self.tracking_score,
            'tw': self.tracking_weight,
            'product_score': self.total_score,
            'percentage_score': self.percentage_score,
            'is_sold_out': self.is_sold_out,
        }


class _ProductListAction(_BaseAction):
    def __init__(
        self,
        raw_product_data: dict,
        position_on_page: int,
        session_id: str,
        user_id: Optional[str] = None,
        user_tier: Optional[dict] = None,
        weight_version: int = None,
        question_score: float = None,
        question_weight: float = None,
        order_score: float = None,
        order_weight: float = None,
        tracking_score: float = None,
        tracking_weight: float = None,
        percentage_score: float = -1,
    ):
        if not isinstance(position_on_page, int):
            raise ArgumentTypeException(self.__init__, 'position_on_page', position_on_page)
        elif position_on_page <= 0:
            raise ArgumentValueException('{} expects {} > 0, but {} is given!'.format(
                self.__init__.__qualname__,
                'position_on_page',
                position_on_page
            ))

        super().__init__(
            raw_product_data,
            session_id,
            user_id,
            user_tier,
            weight_version=weight_version,
            question_score=question_score,
            question_weight=question_weight,
            order_score=order_score,
            order_weight=order_weight,
            tracking_score=tracking_score,
            tracking_weight=tracking_weight,
            percentage_score=percentage_score,
        )

        self.__position_on_page = position_on_page

    @property
    def position_on_page(self) -> int:
        return self.__position_on_page

    @property
    def action_data(self) -> dict:
        action_data = super().action_data
        action_data['position_on_page'] = self.position_on_page
        return action_data

    def to_dict(self) -> dict:
        result = super(_ProductListAction, self).to_dict()
        result['position_on_page'] = self.position_on_page
        return result


class ViewAction(_ProductListAction):
    pass


class ClickAction(_ProductListAction):
    pass


class VisitAction(_BaseAction):
    pass


class ProductsTrackingModel(object):
    @classmethod
    def track(cls, action_or_list: Union[_BaseAction, List[_BaseAction]]) -> None:
        actions_map = {
            ViewAction: {
                'type': ACTION_TYPE.view,
                'counter_name': 'views',
            },
            ClickAction: {
                'type': ACTION_TYPE.click,
                'counter_name': 'clicks',
            },
            VisitAction: {
                'type': ACTION_TYPE.visit,
                'counter_name': 'visits',
            },
        }

        datalake = DataLakeBase()
        buffer = list()

        if isinstance(action_or_list, _BaseAction):
            action = action_or_list
            if action.__class__ not in actions_map.keys():
                raise TypeError('{} does not know, how to work with {} instance!'.format(
                    cls.track.__qualname__,
                    action.__class__.__qualname__
                ))

            item = action.to_dict()
            item.update({
                'action': actions_map[action.__class__]['type'],
            })
            buffer.append(item)

        elif isinstance(action_or_list, list) and all(isinstance(x, _BaseAction) for x in action_or_list):
            for action in action_or_list:
                if action.__class__ not in actions_map.keys():
                    warn('{} does not know, how to work with {} instance!'.format(
                        cls.track.__qualname__,
                        action.__class__.__qualname__
                    ))
                    continue

                item = action.to_dict()
                item.update({
                    'action': actions_map[action.__class__]['type'],
                })
                buffer.append(item)
        else:
            raise ArgumentTypeException(cls.track, 'action', action)

        status, msg = datalake.put_record_batch(buffer)
        if not status:
            warn(msg)
