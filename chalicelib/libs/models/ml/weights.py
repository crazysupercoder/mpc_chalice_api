from decimal import Decimal
from typing import Union
from datetime import datetime


def convert_to_datetime(value: str, format_str: str = '%Y-%m-%d %H:%M:%S'):
    try:
        if isinstance(value, str):
            return datetime.strptime(value, format_str)
        elif isinstance(value, datetime):
            return value
        else:
            return None
    except:
        return None


class ScoringWeight(object):
    __version: int = None
    __question: float = 1.0
    __order: float = 1.0
    __track: float = 1.0
    __time_format: str = '%Y-%m-%d %H:%M:%S'
    __created_at: datetime = datetime.now()
    __expired_at: datetime = None
    __updated_by: str = None

    def __init__(
            self,
            version: Union[str, int] = None,
            question: Union[str, float, int] = 1.0,
            order: Union[str, float, int] = 1.0,
            track: Union[str, float, int] = 1.0,
            created_at: str = datetime.now(),
            expired_at: str = None,
            updated_by: str = None,
            **kwargs):
        self.version = version
        self.question = question
        self.order = order
        self.track = track
        self.created_at = created_at
        self.expired_at = expired_at
        self.__updated_by = updated_by
        super(ScoringWeight, self).__init__()

    @property
    def updated_by(self) -> str:
        return self.__updated_by

    @property
    def question(self) -> float:
        return self.__question
    
    @question.setter
    def question(self, value: Union[float, int]):
        if value and isinstance(value, (int, float, str)):
            try:
                self.__question = float(value)
            except:
                pass

    @property
    def order(self) -> float:
        return self.__order
    
    @order.setter
    def order(self, value: Union[float, int]):
        if value and isinstance(value, (int, float, str)):
            try:
                self.__order = float(value)
            except:
                pass

    @property
    def track(self) -> float:
        return self.__track
    
    @track.setter
    def track(self, value: Union[float, int]):
        if value and isinstance(value, (int, float, str)):
            try:
                self.__track = float(value)
            except:
                pass

    @property
    def created_at(self) -> str:
        return self.__created_at.strftime(self.__time_format)

    @created_at.setter
    def created_at(self, value: Union[str, datetime]):
        self.__created_at = convert_to_datetime(value)

    @property
    def expired_at(self) -> str:
        if isinstance(self.__expired_at, datetime):
            return self.__expired_at.strftime(self.__time_format)
        else:
            return None

    @expired_at.setter
    def expired_at(self, value: Union[str, datetime]):
        self.__expired_at = convert_to_datetime(value)

    @property
    def version(self) -> int:
        return self.__version or 1

    @version.setter
    def version(self, value: Union[int, str, Decimal]):
        if isinstance(value, (int, str, Decimal)):
            self.__version = int(value)

    @property
    def is_initial(self):
        return self.version is None

    def __eq__(self, other):
        attrs_to_match = ['question', 'order', 'track']
        for attr in attrs_to_match:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def to_shorten_dict(self) -> dict:
        return {
            'version': int(self.version),
            'qw': float(self.question),
            'ow': float(self.order),
            'tw': float(self.track),
        }

    def to_dict(self, to_str: bool = True) -> dict:
        if to_str:
            return {
                'version': str(self.version),
                'question': str(self.question),
                'order': str(self.order),
                'track': str(self.track),
                'created_at': self.created_at,
                'expired_at': self.expired_at,
            }
        else:
            return {
                'version': int(self.version) if self.version else None,
                'question': float(self.question),
                'order': float(self.order),
                'track': float(self.track),
                'created_at': self.created_at,
                'expired_at': self.expired_at,
            }
