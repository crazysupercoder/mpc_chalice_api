import boto3
from typing import List
from boto3.dynamodb.conditions import Key, Attr
from ....settings import settings
from .base import DynamoModel
from .genders import Gender


class SizeBase:
    gender = None
    tops: List[str]
    bottoms: List[str]
    shoes: List[str]

    def __init__(
            self,
            gender: str,
            tops: List[str]=['M'],
            bottoms: List[str]=['L'],
            shoes: List[str]=['11'], **kwargs):
        self.gender = gender.upper()
        self.tops = [top.upper() for top in tops]
        self.bottoms = [bottom.upper() for bottom in bottoms]
        self.shoes = [shoe.upper() for shoe in shoes]

    def __str__(self) -> str:
        return "<Size(%s): Tops->%s, Bottoms->%s and Shoes->%s>" % (
            self.gender,
            ','.join(self.tops),
            ','.join(self.bottoms),
            ','.join(self.shoes))

    def to_dict(self) -> dict:
        return {
            "Tops": self.tops,
            "Bottoms": self.bottoms,
            "Shoes": self.shoes,
        }


class MenSize(SizeBase):
    gender = 'MENS'

    def __init__(self, **kwargs):
        self.gender = kwargs.pop('gender', self.gender)
        super(MenSize, self).__init__(self.gender, **kwargs)


class WomenSize(MenSize):
    gender = 'LADIES'
    dresses: List[str]

    def __init__(self, dresses: List[str]=['M'], **kwargs):
        super(WomenSize, self).__init__(**kwargs)
        self.dresses = [dress.upper() for dress in dresses]

    def to_dict(self) -> dict:
        result = super(WomenSize, self).to_dict()
        result['Dresses'] = self.dresses
        return result


class KidsSize(WomenSize):
    gender = 'KIDS'


class SizeOptions:
    genders: List[str] = []
    men: MenSize = None
    women: WomenSize = None
    kids: KidsSize = None
    activate_filter: bool = True

    def __init__(self,
            data: dict=None, activate_filter=True,
            genders: List[str]=[Gender.women]):
        if data is None:
            self.genders = [gender.upper() for gender in genders]
            if Gender.men in self.genders:
                self.men = MenSize()
            if Gender.women in self.genders:
                self.women = WomenSize()
            if Gender.kids in self.genders:
                self.kids = KidsSize()
        else:
            sizes = data.get('sizes', {})
            self.activate_filter = data.get('activate_filter', self.activate_filter)
            for gender, options in sizes.items():
                refined_options = dict()
                for key, value in options.items():
                    refined_options[key.lower()] = value

                if gender.upper() == Gender.men:
                    self.genders.append(Gender.men)
                    self.men = MenSize(**refined_options)
                elif gender.upper() == Gender.women:
                    self.genders.append(Gender.women)
                    self.women = WomenSize(**refined_options)
                elif gender.upper() == Gender.kids:
                    self.genders.append(Gender.kids)
                    self.kids = KidsSize(**refined_options)
                else:
                    Warning("Unknown gender %s found." % gender)

    def to_dict(self) -> dict:
        sizes = dict()
        if self.men is not None:
            sizes[Gender.men] = self.men.to_dict()
        if self.women is not None:
            sizes[Gender.women] = self.women.to_dict()
        if self.kids is not None:
            sizes[Gender.kids] = self.kids.to_dict()
        return {
            "activate_filter": self.activate_filter,
            "sizes": sizes}


class ProductSize(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PRODUCT_SIZE'

    def __init__(self):
        super(ProductSize, self).__init__(self.TABLE_NAME)

    def filter_by_product_size_ids(self, ids):
        return self.filter_by_field_in_array('product_size_id', ids, value_type=int)['Items']

    def filter_by_product_size_name(self, sizes):
        if len(sizes) == 0:
            return []

        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key()),
            FilterExpression=Attr('product_size_name').is_in(sizes)
        )
        return [item['sk'] for item in response['Items']]
