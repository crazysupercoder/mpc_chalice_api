from boto3.dynamodb.conditions import Key
from typing import Tuple
from .....settings import settings
from ..base import DynamoModel


class UserQuestionEntitySubOption(object):
    TYPE_TEXT = 'text'
    TYPE_SVG = 'svg'

    def __init__(self, uqo_id: str, uqo_type: str, value: str, png_image: str, svg_image: str, entity: str, 
        entity_id: str, fixel_category_id: str, fixel_category_name: str, brand_id: str, brand_name: str):
        if len(uqo_id) <= 0:
            raise ValueError(self.__class__.__name__ + ' instance "id" is incorrect!')

        if uqo_type not in (self.TYPE_TEXT, self.TYPE_SVG):
            raise ValueError(self.__class__.__name__ + ' instance "type" "' + uqo_type + '" is unknown!')

        if len(value) <= 0 and len(svg_image) <= 0:
            raise ValueError(self.__class__.__name__ + ' instance requires "value" or "svg_image" parameter!')

        self.__id = uqo_id
        self.__type = uqo_type
        self.__value = value
        self.__png_image = png_image
        self.__svg_image = svg_image
        self.__entity = entity
        self.__entity_id = entity_id
        self.__fixel_category_id = fixel_category_id
        self.__fixel_category_name = fixel_category_name
        self.__brand_id = brand_id
        self.__brand_name = brand_name

    @property
    def id(self):
        return int(self.__id)

    @property
    def type(self):
        return self.__type

    @property
    def value(self):
        return self.__value

    @property
    def png_image(self):
        return self.__png_image

    @property
    def svg_image(self):
        return self.__svg_image

    @property
    def entity(self):
        return self.__entity

    @property
    def entity_id(self):
        return self.__entity_id

    @property
    def fixel_category_id(self):
        return self.__fixel_category_id

    @property
    def fixel_category_name(self):
        return self.__fixel_category_name

    @property
    def brand_id(self):
        return self.__brand_id

    @property
    def brand_name(self):
        return self.__brand_name

class UserQuestionEntityOption(object):
    TYPE_TEXT = 'text'
    TYPE_SVG = 'svg'

    def __init__(self, uqo_id: str, uqo_type: str, value: str, png_image: str, svg_image: str, entity: str, 
        entity_id: str, fixel_category_id: str, fixel_category_name: str, brand_id: str, brand_name: str,
        suboptions: Tuple[UserQuestionEntitySubOption, ...],):
        if len(uqo_id) <= 0:
            raise ValueError(self.__class__.__name__ + ' instance "id" is incorrect!')

        if uqo_type not in (self.TYPE_TEXT, self.TYPE_SVG):
            raise ValueError(self.__class__.__name__ + ' instance "type" "' + uqo_type + '" is unknown!')

        if len(value) <= 0 and len(svg_image) <= 0:
            raise ValueError(self.__class__.__name__ + ' instance requires "value" or "svg_image" parameter!')

        self.__id = uqo_id
        self.__type = uqo_type
        self.__value = value
        self.__png_image = png_image
        self.__svg_image = svg_image
        self.__entity = entity
        self.__entity_id = entity_id
        self.__fixel_category_id = fixel_category_id
        self.__fixel_category_name = fixel_category_name
        self.__brand_id = brand_id
        self.__brand_name = brand_name
        self.__suboptions = suboptions

    @property
    def id(self):
        return int(self.__id)

    @property
    def type(self):
        return self.__type

    @property
    def value(self):
        return self.__value

    @property
    def png_image(self):
        return self.__png_image

    @property
    def svg_image(self):
        return self.__svg_image

    @property
    def entity(self):
        return self.__entity

    @property
    def entity_id(self):
        return self.__entity_id

    @property
    def fixel_category_id(self):
        return self.__fixel_category_id

    @property
    def fixel_category_name(self):
        return self.__fixel_category_name

    @property
    def brand_id(self):
        return self.__brand_id

    @property
    def brand_name(self):
        return self.__brand_name

    @property
    def suboptions(self) -> Tuple[UserQuestionEntitySubOption, ...]:
        return self.__suboptions

class USER_QUESTION_TYPE:
    customer = 'customer'
    product = 'product'


class UserQuestionEntity(object):
    TYPE_SELECT = 'Select'
    TYPE_INPUT = 'Input'

    def __init__(
        self,
        uq_id: str,
        uq_type: str,
        question: str,
        priority: int,
        attribute_type: str,
        attribute_value: str,
        options: Tuple[UserQuestionEntityOption, ...],
    ):
        if uq_type not in (self.TYPE_SELECT, self.TYPE_INPUT):
            raise ValueError(self.__class__.__name__ + ' instance "type" "' + uq_type + '" is unknown!')

        self.__id = uq_id
        self.__type = uq_type
        self.__question = question
        self.__priority = priority
        self.__attribute_type = attribute_type
        self.__attribute_value = attribute_value 
        self.__options = options

    @property
    def id(self) -> str:
        return self.__id

    @property
    def type(self) -> str:
        return self.__type

    @property
    def priority(self) -> int:
        return self.__priority

    @property
    def question(self):
        return self.__question

    @property
    def attribute_type(self) -> str:
        return self.__attribute_type

    @property
    def attribute_value(self) -> str:
        return self.__attribute_value

    @property
    def options(self) -> Tuple[UserQuestionEntityOption, ...]:
        return self.__options

# ----------------------------------------------------------------------------------------------------------------------


class UserQuestionModel(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'USER_QUESTION'

    def __init__(self):
        super(UserQuestionModel, self).__init__(self.TABLE_NAME)

    def save(self, entity: UserQuestionEntity) -> None:
        data = {
            'pk': self.get_partition_key(),
            'sk': entity.id,
            'type': entity.type,
            'question': entity.question,
            'priority': entity.priority,
            'attribute_type': entity.attribute_type,
            'attribute_value': entity.attribute_value,
            'options': list(map(lambda option: {
                'id': option.id,
                'type': option.type,
                'value': option.value,
                'png_image': option.png_image if option.png_image != "None" else None,
                'svg_image': option.svg_image if option.png_image != "None" else None,
                'entity': option.entity if option.entity != "None" else None,
                'entity_id': option.entity_id,
                'fixel_category_id':option.fixel_category_id if option.fixel_category_id != "None" else None,
                'fixel_category_name':option.fixel_category_name if option.fixel_category_name != "None" else None,
                'brand_id':option.brand_id if option.brand_id != "None" else None,
                'brand_name':option.brand_name if option.brand_name != "None" else None,
                'suboptions': list(map(lambda suboption: {
                    'id': suboption.id,
                    'type': suboption.type,
                    'value': suboption.value,
                    'png_image': suboption.png_image,
                    'svg_image': suboption.svg_image,
                    'entity': suboption.entity,
                    'entity_id': suboption.entity_id,
                    'fixel_category_id':suboption.fixel_category_id,
                    'fixel_category_name':suboption.fixel_category_name,
                    'brand_id':suboption.brand_id,
                    'brand_name':suboption.brand_name,
                }, option.suboptions)) if option.suboptions else None
            }, entity.options)) if entity.options else None,
        }

        # insert or update
        self.table.put_item(Item=data)

    def delete(self, question_id):
        key = {
            'pk': self.get_partition_key(),
            'sk': question_id
        }
        response = self.table.delete_item(
            Key=key
        )
        return response

    def get_all(self, convert=True, **kwargs) -> Tuple[UserQuestionEntity, ...]:
        db_response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key()))
        rows = db_response['Items']

        result = tuple(map(lambda row: UserQuestionEntity(
            str(row.get('sk')),
            str(row.get('type')),
            str(row.get('question')),
            int(row.get('priority')),
            str(row.get('attribute_type')),
            str(row.get('attribute_value')),
            tuple(map(lambda option: UserQuestionEntityOption(
                str(option.get('id')),
                str(option.get('type')),
                str(option.get('value')),
                str(option.get('png_image')),
                str(option.get('svg_image')),
                str(option.get('entity')),
                str(option.get('entity_id')),
                str(option.get('fixel_category_id')),
                str(option.get('fixel_category_name')),
                str(option.get('brand_id')),
                str(option.get('brand_name')),
                tuple(map(lambda suboption: UserQuestionEntitySubOption(
                    str(suboption.get('id')),
                    str(suboption.get('type')),
                    str(suboption.get('value')),
                    str(suboption.get('png_image')),
                    str(suboption.get('svg_image')),
                    str(suboption.get('entity')),
                    str(suboption.get('entity_id')),
                    str(suboption.get('fixel_category_id')),
                    str(suboption.get('fixel_category_name')),
                    str(suboption.get('brand_id')),
                    str(suboption.get('brand_name')),
                ), option.get('suboptions'))) if option.get('suboptions') else None
            ), row.get('options'))) if row.get('options') else None
        ), rows))

        if convert:
            return [self.__convert(item) for item in result]
        else:
            return result

    def __convert(self, questionEntity: UserQuestionEntity) -> dict:
        return {
            'id': questionEntity.id,
            # 'type': questionEntity.type,
            'question': questionEntity.question,
            'priority': questionEntity.priority,
            'attribute': {
                'type': questionEntity.attribute_type,
                'value': questionEntity.attribute_value,
            },
            'options': [{
                'id': option.id,
                # 'type': option.type,
                'value': option.value,
                'png_image': option.png_image,
                'svg_image': option.svg_image,
                # 'entity': option.entity,
                # 'entity_id': option.entity_id,
                # 'fixel_category': {
                #     'id': option.fixel_category_id,
                #     'name': option.fixel_category_name
                # },
                # 'brand': {
                #     'id': option.brand_id,
                #     'name': option.brand_name
                # },
                'suboptions': [{
                    'id': suboption.id,
                    # 'type': suboption.type,
                    'value': suboption.value,
                    'png_image': suboption.png_image,
                    'svg_image': suboption.svg_image,
                    # 'entity': suboption.entity,
                    # 'entity_id': suboption.entity_id,
                    # 'fixel_category': {
                    #     'id': suboption.fixel_category_id,
                    #     'name': suboption.fixel_category_name
                    # },
                    # 'brand': {
                    #     'id': suboption.brand_id,
                    #     'name': suboption.brand_name
                    # }
                } for suboption in option.suboptions] if option.suboptions else []
            } for option in questionEntity.options] if questionEntity.options else [],
        }

    def get_item(self, sk):
        item = super(UserQuestionModel, self).get_item(sk)['Item']
        instance = UserQuestionEntity(
            str(item.get('sk')),
            str(item.get('type')),
            str(item.get('question')),
            int(item.get('priority')),
            str(item.get('attribute_type')),
            str(item.get('attribute_value')),
            tuple(map(lambda option: UserQuestionEntityOption(
                str(option.get('id')),
                str(option.get('type')),
                str(option.get('value')),
                str(option.get('png_image')),
                str(option.get('svg_image')),
                str(option.get('entity')),
                str(option.get('entity_id')),
                str(option.get('fixel_category_id')),
                str(option.get('fixel_category_name')),
                str(option.get('brand_id')),
                str(option.get('brand_name')),
                tuple(map(lambda suboption: UserQuestionEntitySubOption(
                    str(suboption.get('id')),
                    str(suboption.get('type')),
                    str(suboption.get('value')),
                    str(suboption.get('png_image')),
                    str(suboption.get('svg_image')),
                    str(suboption.get('entity')),
                    str(suboption.get('entity_id')),
                    str(suboption.get('fixel_category_id')),
                    str(suboption.get('fixel_category_name')),
                    str(suboption.get('brand_id')),
                    str(suboption.get('brand_name')),
                ), option.get('suboptions'))) if option.get('suboptions') else None
            ), item.get('options'))) if item.get('options') else None
        )
        return self.__convert(instance)

# ----------------------------------------------------------------------------------------------------------------------

