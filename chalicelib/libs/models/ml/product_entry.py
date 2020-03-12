import math
import random
from warnings import warn
from datetime import datetime, timedelta
from typing import List, Union, Optional
from decimal import Decimal
from chalicelib.settings import settings
from chalicelib.libs.models.mpc.Cms.UserQuestions import UserQuestionEntity as Question
from chalicelib.libs.core.datetime import DATETIME_FORMAT
from .questions import Answer
from .weights import ScoringWeight
from .orders import OrderAggregation
from .tracks import UserTrackEntry


class ProductSize:
    size: str
    qty: int
    rs_simple_sku: str
    portal_simple_id: str
    status: int

    def __init__(
            self,
            size: Union[str, dict]=None,
            qty: int=None,
            rs_simple_sku: str=None,
            portal_simple_id: str=None,
            status: int = None,
            **kwargs):
        if isinstance(size, str):
            self.size = size
            self.qty = qty
            self.rs_simple_sku = rs_simple_sku
            self.portal_simple_id = portal_simple_id
        elif isinstance(size, dict) and 'size' in size.keys():
            self.size = size.get('size')
            self.qty = size.get('qty')
            self.rs_simple_sku = size.get('rs_simple_sku')
            self.portal_simple_id = size.get('portal_simple_id')
            self.status = size.get('status')

    def to_dict(self) -> dict:
        return {
            'size': self.size,
            'qty': self.qty,
            'rs_simple_sku': self.rs_simple_sku,
            'portal_simple_id': self.portal_simple_id
        }


class ProductImage:
    s3_filepath: str
    position: str
    delete: int

    def __init__(
            self,
            s3_filepath: Union[str, dict] = None,
            position: str = None,
            delete: int = 0, **kwargs):
        if isinstance(s3_filepath, str):
            self.s3_filepath = s3_filepath
            self.position = position
            self.delete = delete
        elif isinstance(s3_filepath, dict) and 's3_filepath' in s3_filepath.keys():
            self.s3_filepath = s3_filepath.get('s3_filepath')
            self.position = s3_filepath.get('position')
            self.delete = s3_filepath.get('delete')

    def to_dict(self) -> dict:
        return {
            "s3_filepath": self.s3_filepath,
            "position": self.position,
            "delete": self.delete,
        }


class PercentageScoreRange:
    max_score: float = -float('inf')
    min_score: float = float('inf')


class ProductEntry(object):
    portal_config_id: int
    event_code: str
    manufacturer: str
    season: str
    product_size_attribute: str
    rs_product_sub_type: str
    rs_colour: str
    gender: str
    product_name: str
    size_chart: str
    neck_type: str
    fit: str
    dimensions: str
    sticker_id: str
    fabrication: str
    size_fit: str
    product_description: str
    rs_sku: str
    rs_selling_price: float
    discount: float
    freebie: bool
    status: int
    created_at: str
    updated_at: str
    sizes: List[ProductSize]
    images: List[ProductImage]
    img: dict
    brand_code: str
    __weights_version: int = None
    __questions_score: int = 0
    __questions_weight: float = 1.0
    __orders_score: int = 0
    __orders_weight: float = 1.0
    __tracking_score: int = 0
    __tracking_weight: float = 1.0

    views: int = 0
    clicks: int = 0
    visits: int = 0
    __viewed_at: datetime = None

    score_range: PercentageScoreRange = None

    @property
    def question_score(self):
        return self.__questions_score

    @question_score.setter
    def question_score(self, value: Union[int, float, str]):
        self.__questions_score = int(value)

    @property
    def order_score(self):
        return self.__orders_score

    @order_score.setter
    def order_score(self, value: Union[int, float, str]):
        self.__orders_score = int(value)

    @property
    def tracking_score(self):
        return self.__tracking_score

    @tracking_score.setter
    def tracking_score(self, value: Union[int, float, str]):
        self.__tracking_score = int(value)

    def __init__(
            self,
            portal_config_id: int=None,
            event_code: str=None,
            manufacturer: str=None,
            season: str=None,
            product_size_attribute: str=None,
            rs_product_sub_type: str=None,
            rs_colour: str=None,
            gender: str=None,
            product_name: str=None,
            size_chart: str=None,
            neck_type: str=None,
            fit: str=None,
            dimensions: str=None,
            sticker_id: str=None,
            fabrication: str=None,
            size_fit: str=None,
            product_description: str=None,
            rs_sku: str=None,
            rs_selling_price: float=None,
            discount: float=None,
            freebie: bool=None,
            status: int=None,
            created_at: str=None,
            updated_at: str=None,
            sizes: List[dict]=[],
            images: List[dict]=[],
            img: dict=None,
            brand_code: str=None,
            question_score: float = None,
            order_score: float = None,
            tracking_score: float = None,
            views: int = 0,
            clicks: int = 0,
            visits: int = 0,
            viewed_at: Union[datetime, str] = None,
            **kwargs):

        self.portal_config_id = portal_config_id
        self.event_code = event_code
        self.manufacturer = manufacturer
        self.season = season
        self.product_size_attribute = product_size_attribute
        self.rs_product_sub_type = rs_product_sub_type
        self.rs_colour = rs_colour
        self.gender = gender
        self.product_name = product_name
        self.size_chart = size_chart
        self.neck_type = neck_type
        self.fit = fit
        self.dimensions = dimensions
        self.sticker_id = sticker_id
        self.fabrication = fabrication
        self.size_fit = size_fit
        self.product_description = product_description
        self.rs_sku = rs_sku
        self.rs_selling_price = rs_selling_price
        self.discount = discount
        self.freebie = freebie
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
        self.sizes = [ProductSize(size) for size in sizes]
        self.images = [ProductImage(image) for image in images]
        self.img = img
        self.brand_code = brand_code
        if viewed_at:
            if isinstance(viewed_at, str):
                self.viewed_at = datetime.strptime(viewed_at, DATETIME_FORMAT)
            elif isinstance(viewed_at, datetime):
                self.viewed_at = viewed_at

    @property
    def viewed_at(self) -> datetime:
        return self.__viewed_at

    @viewed_at.setter
    def viewed_at(self, value: Optional[datetime]):
        self.__viewed_at = value

    @property
    def original_price(self) -> float:
        try:
            return float(str(self.rs_selling_price))
        except:
            return 0.0

    @property
    def current_price(self) -> float:
        try:
            original_price = self.original_price
            discount = float(str(self.discount))
            current_price = original_price - original_price * discount / 100
            return current_price
        except:
            return 0.0

    def to_dict(self, mode: str = 'detail', tier: dict = None) -> dict:
        if mode == 'detail':
            return {
                "portal_config_id": self.portal_config_id,
                "event_code": self.event_code,
                "manufacturer": self.manufacturer,
                "season": self.season,
                "product_size_attribute": self.product_size_attribute,
                "rs_product_sub_type": self.rs_product_sub_type,
                "rs_colour": self.rs_colour,
                "gender": self.gender,
                "product_name": self.product_name,
                "size_chart": self.size_chart,
                "neck_type": self.neck_type,
                "fit": self.fit,
                "dimensions": self.dimensions,
                "sticker_id": self.sticker_id,
                "fabrication": self.fabrication,
                "size_fit": self.size_fit,
                "product_description": self.product_description,
                "rs_sku": self.rs_sku,
                "rs_selling_price": self.rs_selling_price,
                "discount": self.discount,
                "freebie": self.freebie,
                "status": self.status,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "sizes": [size.to_dict() for size in self.sizes],
                "images": [image.to_dict() for image in self.images],
                "img": self.img,
                "brand_code": self.brand_code,
                "original_price": self.original_price,
                "current_price": self.current_price,
            }
        elif mode == 'scored':
            return {
                "portal_config_id": self.portal_config_id,
                "event_code": self.event_code,
                "manufacturer": self.manufacturer,
                "season": self.season,
                "product_size_attribute": self.product_size_attribute,
                "rs_product_sub_type": self.rs_product_sub_type,
                "rs_colour": self.rs_colour,
                "gender": self.gender,
                "product_name": self.product_name,
                "size_chart": self.size_chart,
                "neck_type": self.neck_type,
                "fit": self.fit,
                "dimensions": self.dimensions,
                "sticker_id": self.sticker_id,
                "fabrication": self.fabrication,
                "size_fit": self.size_fit,
                "product_description": self.product_description,
                "rs_sku": self.rs_sku,
                "rs_selling_price": self.rs_selling_price,
                "discount": self.discount,
                "freebie": self.freebie,
                "status": self.status,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "sizes": [size.to_dict() for size in self.sizes],
                "images": [image.to_dict() for image in self.images],
                "img": self.img,
                "brand_code": self.brand_code,
                "original_price": self.original_price,
                "current_price": self.current_price,
                "question_score": self.question_score,
                "order_score": self.order_score,
                "tracking_score": self.tracking_score,
                "total_score": self.total_score,
                "percentage_score": self.percentage_score,
                "tracking_info": {
                    "views": self.views,
                    "clicks": self.clicks,
                    "visits": self.visits,
                },
                "viewed_at": self.viewed_at.strftime(DATETIME_FORMAT)
                    if isinstance(self.viewed_at, datetime)
                    else (self.viewed_at 
                        if isinstance(self.viewed_at, str)
                        else None),
                "is_seen": (self.views > 0 or self.clicks > 0 or self.visits > 0),
            }
        else:
            image_src = None
            if len(self.images) > 0:
                image_src = self.images[0].s3_filepath
                if isinstance(image_src, dict):
                    image_src = image_src.get('s3_filepath')
            image_src = image_src or 'https://placeimg.com/155/140/arch'

            from_date = datetime.now() - timedelta(days=settings.NEW_PRODUCT_THRESHOLD)
            item = {
                'id': self.portal_config_id,
                'sku': self.rs_sku,
                'title': self.product_name,
                'subtitle': self.product_description,
                'price': Decimal(0 if self.rs_selling_price is None else self.rs_selling_price),
                'badge': 'NEW IN' if datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S") > from_date else None,
                'favorite': random.choice([True, False]),
                'product_type': self.product_size_attribute,
                'product_sub_type': self.rs_product_sub_type,
                'gender': self.gender,
                'brand': self.manufacturer,
                'scores': {
                    'version': self.__weights_version,
                    'qs': self.question_score,
                    'qw': self.__questions_weight,
                    'rs': self.order_score,
                    'rw': self.__orders_weight,
                    'ts': self.tracking_score,
                    'tw': self.__tracking_weight,
                    'total': self.total_score,
                    'percentage_score': self.percentage_score,
                },
                # 'score': self.total_score,
                'sizes': [
                    {
                        'size': size.size['size'],
                        'qty': size.size['qty'],
                        'rs_simple_sku': size.size['rs_simple_sku']
                    } if isinstance(size.size, dict) else
                    {
                        'size': size.size,
                        'qty': size.qty,
                        'rs_simple_sku': size.rs_simple_sku
                    }
                    for size in self.sizes],
                'image': {
                    'src': image_src,
                    'title': self.product_size_attribute,
                },
                "original_price": self.original_price,
                "current_price": self.current_price,
            }

            if tier is not None and type(tier) == dict:
                item['fbucks'] = math.ceil(item['price'] * tier.get('discount_rate') / 100)
            return item

    def __check_attribute(self, attr_name: str, values: List[str]):
        if not isinstance(values, list):
            values = [values]
        if attr_name == 'sizes':
            for size in self.sizes:
                if size.size in values:
                    return True
            else:
                return False
        else:
            return str(getattr(self, attr_name)).lower() in [
                str(value).lower() for value in values if value]

    def apply_questions(self, answers: List[Answer]):
        for answer in answers:
            # target_attr: str = question.target_attr
            # answers: List[str] = question.answers
            if not isinstance(answers, list):
                answers = [answers]

            for query in answer.queries:
                for key, values in query.items():
                    if not hasattr(self, key):
                        print("Unknown attr - %s found." % key)
                        # raise Exception("Unknown attr - %s found." % key)

                    if not self.__check_attribute(key, values):
                        break
                else:
                    self.question_score += answer.question_score
                    break
            else:
                self.question_score -= answer.question_score
            # if not hasattr(self, target_attr):
            #     raise Exception("Unknown attr - %s found." % target_attr)
            # if str(getattr(self, target_attr)).lower() in [
            #         answer.lower() for answer in answers]:
            #     self.question_score += question.question_score
            # else:
            #     self.question_score -= question.question_score

    def apply_orders(self, orders: OrderAggregation):
        if self.gender.lower() in orders.genders:
            self.order_score += orders.gender_score
        else:
            self.order_score -= orders.gender_score

        if self.manufacturer.lower() in orders.brands:
            self.order_score += orders.brand_score
        else:
            self.order_score -= orders.brand_score

        if self.product_size_attribute.lower() in orders.product_types:
            self.order_score += orders.product_type_score
        else:
            self.order_score -= orders.product_type_score

        if str(self.rs_colour).lower() in [str(c).lower() for c in orders.colors]:
            self.order_score += orders.color_score
        else:
            self.order_score -= orders.color_score

        for size in self.sizes:
            if isinstance(size.size, dict) and 'size' in size.size.keys() and\
                    str(size.size['size']).lower() in [str(s).lower() for s in orders.sizes]:
                self.order_score += orders.size_score
                break
            elif isinstance(size.size, str) and size.size.lower() in [str(s).lower() for s in orders.sizes]:
                self.order_score += orders.size_score
                break
            else:
                continue
        else:
            self.order_score -= orders.size_score

    def apply_trackings(self, trackings: UserTrackEntry):
        if self.product_size_attribute.lower() in trackings.product_types:
            self.tracking_score += trackings.product_type_score
        else:
            self.tracking_score -= trackings.product_type_score

        if self.rs_product_sub_type.lower() in trackings.product_sub_types:
            self.tracking_score += trackings.product_sub_type_score
        else:
            self.tracking_score -= trackings.product_sub_type_score

        if self.brand_code.lower() in trackings.brands:
            self.tracking_score += trackings.brand_score
        else:
            self.tracking_score -= trackings.brand_score

        if self.gender.lower() in trackings.genders:
            self.tracking_score += trackings.gender_score
        else:
            self.tracking_score -= trackings.gender_score

        for size in self.sizes:
            if isinstance(size.size, dict) and 'size' in size.size.keyw() and\
                    str(size.size['size']).lower() in trackings.sizes:
                self.tracking_score += trackings.size_score
                break
            elif isinstance(size.size, str) and size.size.lower() in trackings.sizes:
                self.tracking_score += trackings.size_score
            else:
                continue
        else:
            self.tracking_score -= trackings.size_score

    @property
    def total_score(self):
        return sum([
            self.question_score * self.__questions_weight,
            self.order_score * self.__orders_weight,
            self.tracking_score * self.__tracking_weight
        ])

    @property
    def percentage_score(self) -> float:
        if not isinstance(self.score_range, PercentageScoreRange):
            return 0
        elif self.score_range.min_score == self.score_range.max_score:
            warn("Unexpected case found here. MAX == MIN in score range")
            return 0
        else:
            return float("%.2f" % (
                (self.total_score - self.score_range.min_score) * 100 / (
                self.score_range.max_score - self.score_range.min_score)))

    def set_weights(self, weights: ScoringWeight):
        self.__weights_version = weights.version
        self.__questions_weight = weights.question
        self.__orders_weight = weights.order
        self.__tracking_weight = weights.track

    def weighted_total_score(
            self,
            weights: ScoringWeight = None,
            version: int = None,
            qw: float = 1.0,
            ow: float = 1.0,
            tw: float = 1.0):
        if weights is None:
            self.__weights_version = version
            self.__questions_weight = qw
            self.__orders_weight = ow
            self.__tracking_weight = tw
        elif isinstance(weights, ScoringWeight):
            self.set_weights(weights)
        else:
            raise Exception("Unexpected case found.")
        return sum([
            self.question_score * qw,
            self.order_score * ow,
            self.tracking_score * tw
        ])
