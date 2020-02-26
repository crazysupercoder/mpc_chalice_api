from typing import Union, Dict
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr, Between, GreaterThanEquals, LessThanEquals
from typing import List, Tuple
from chalicelib.settings import settings
from ..base import DynamoModel, boto3
from ...ml.weights import ScoringWeight, convert_to_datetime


class WeightModel(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'SCORING_WEIGHT'
    CURRENT_SCORING_WEIGHTS_SK = 'CURRENT'
    __scoring_weights_attr_name = 'weights'
    __updated_by: str = None

    def __init__(self, email: str = None):
        self.__updated_by = email
        super(WeightModel, self).__init__(self.TABLE_NAME)

    def retrieve_weights(
            self,
            from_date: str,
            to_date: str) -> List[ScoringWeight]:
        response = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.PARTITION_KEY),
            FilterExpression=Attr('created_at').lte(to_date) & Attr('expired_at').gte(from_date)
        )
        matches = [ScoringWeight(**item) for item in response.get('Items', [])]
        current = self.scoring_weight
        if current.created_at < to_date and current.created_at > from_date:
            matches += [current]
        return matches

    def register_history(self, item: ScoringWeight) -> bool:
        try:
            data = item.to_dict(to_str=True)
            data['pk'] = self.get_partition_key()
            data['sk'] = str(item.version)
            data['updated_by'] = self.__updated_by
            response = self.table.put_item(Item=data)
            return True
        except Exception as e:
            print(str(e))
            return False

    def save_current(self, item: ScoringWeight) -> bool:
        try:
            response = self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': self.CURRENT_SCORING_WEIGHTS_SK,
            }, AttributeUpdates={
                self.__scoring_weights_attr_name: {'Value': item.to_dict()},
            })
            return True
        except Exception as e:
            print(str(e))
            return False

    @property
    def scoring_weight(self) -> ScoringWeight:
        item = self.get_item(self.CURRENT_SCORING_WEIGHTS_SK).get('Item', {})
        if item.get(self.__scoring_weights_attr_name) and\
                isinstance(item[self.__scoring_weights_attr_name], dict):
            return ScoringWeight(**item.get(self.__scoring_weights_attr_name))
        else:
            return ScoringWeight()

    @scoring_weight.setter
    def scoring_weight(self, value: Dict[str, Union[int, float]]):
        origin = self.scoring_weight
        new = ScoringWeight(**value)
        if origin.is_initial:
            print("Should be saved the current weights.")
            new.version = 1
            self.save_current(new)
        elif origin != new:
            print("Should be recorded.")
            origin.expired_at = new.created_at
            new.version = origin.version + 1
            self.save_current(new)
            self.register_history(origin)
        else:
            print("Skipping because the same value.")
