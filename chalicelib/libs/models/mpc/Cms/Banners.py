from ..base import Base
from boto3.dynamodb.conditions import Key
from datetime import datetime
from chalicelib.settings import settings


class Banners(Base):
    table = None

    def __init__(self):
        Base.__init__(self)
        self.table = self.dynamodb.Table(settings.AWS_DYNAMODB_BANNER_TABLE_NAME)

    def insert(self, item):
        return self.table.put_item(Item=item)

    def get(self, banner_id):
        item = self.table.query(
            KeyConditionExpression=Key('banner_id').eq(int(banner_id))
        )['Items']
        return item

    def listAll(self):
        items = self.table.scan()
        if items.__len__():
            return items['Items']
        return None

    def update(self, banner_id, update_fields):
        updateExpression = 'set'
        for key in update_fields.keys(): 
            updateExpression += ' '
            updateExpression += key
            updateExpression += ' = :update_'
            updateExpression += key
            updateExpression += ','
        updateExpression = updateExpression[:-1]

        expressionAttributeValues = {}
        for key, value in update_fields.items(): 
            newKey = ':update_' + key
            expressionAttributeValues[newKey] = value
        response = self.table.update_item(
            Key={
                'banner_id': int(banner_id)
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues="UPDATED_NEW"
        )
        return response

    def delete(self, banner_id):
        key = { 
            'banner_id': int(banner_id)
        }
        response = self.table.delete_item(
            Key=key
        ) 
        return response

    def list(self, query):
        currenttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        expressionAttributeValues = {
            ":activeValue": 1,
            ":start_dateValue": currenttime,
            ":end_dateValue": currenttime,
        }
        filterExpression  = "active = :activeValue AND start_date < :start_dateValue AND end_date > :end_dateValue"
        if query is not None:
            for key, value in query.items():
                if key == 'gender':
                    filterExpression += ' AND contains(gender, :genderValue)'
                    expressionAttributeValues[':genderValue'] = value
        response = self.table.scan(
            FilterExpression = filterExpression,
            ExpressionAttributeValues = expressionAttributeValues
        )['Items']
        return response
