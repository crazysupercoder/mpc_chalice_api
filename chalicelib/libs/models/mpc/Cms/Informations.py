from typing import List, Optional
from chalicelib.settings import settings
from chalicelib.extensions import *
from ..base import DynamoModel, Base
import hashlib


class InformationAddress:

    def __init__(self, **kwargs):
        kwargsdict = {}
        expected_args = [
            "address_hash", "address_nickname", "recipient_name", "mobile_number",
            "business_name", "complex_building", "street_address",
            "suburb", "postal_code", "city",
            "province", "special_instructions", "business_type",
            "is_default_billing", "is_default_shipping"
        ]
        kwargs_keys = kwargs.keys()
        for key in expected_args:
            if key in kwargs_keys:
                kwargsdict[key] = kwargs[key]
            else:
                if key == 'business_type' or key == 'is_default_billing' or key == 'is_default_shipping':
                    kwargsdict[key] = False
                else:
                    kwargsdict[key] = None

        self.__address_nickname= kwargsdict.get('address_nickname')
        self.__recipient_name = kwargsdict.get('recipient_name')
        self.__mobile_number = kwargsdict.get('mobile_number')
        self.__business_name = kwargsdict.get('business_name')
        self.__complex_building = kwargsdict.get('complex_building')
        self.__street_address = kwargsdict.get('street_address')
        self.__suburb = kwargsdict.get('suburb')
        self.__postal_code = kwargsdict.get('postal_code')
        self.__city = kwargsdict.get('city')
        self.__province = kwargsdict.get('province')
        self.__special_instructions = kwargsdict.get('special_instructions')
        self.__business_type = kwargsdict.get('business_type')
        self.__is_default_billing = kwargsdict.get('is_default_billing')
        self.__is_default_shipping = kwargsdict.get('is_default_shipping')

        if  kwargsdict.get('address_hash'):
            self.__address_hash = kwargsdict.get('address_hash')
        else:
            self.__address_hash = hashlib.md5(str(self.__dict__).encode('utf-8')).hexdigest()

    @property
    def address_nickname(self):
        return self.__address_nickname

    @address_nickname.setter
    def address_nickname(self, value: str):
        self.__address_nickname = value

    @property
    def recipient_name(self):
        return self.__recipient_name

    @recipient_name.setter
    def recipient_name(self, value: str):
        self.__recipient_name = value

    @property
    def mobile_number(self):
        return self.__mobile_number

    @mobile_number.setter
    def mobile_number(self, value: str):
        self.__mobile_number = value

    @property
    def business_name(self):
        return self.__business_name

    @business_name.setter
    def business_name(self, value: str):
        self.__business_name = value

    @property
    def complex_building(self):
        return self.__complex_building

    @complex_building.setter
    def complex_building(self, value: str):
        self.__complex_building = value

    @property
    def street_address(self):
        return self.__street_address

    @street_address.setter
    def street_address(self, value: str):
        self.__street_address = value

    @property
    def suburb(self):
        return self.__suburb

    @suburb.setter
    def suburb(self, value: str):
        self.__suburb = value

    @property
    def postal_code(self):
        return self.__postal_code

    @postal_code.setter
    def postal_code(self, value: str):
        self.__postal_code = value

    @property
    def city(self):
        return self.__city

    @city.setter
    def city(self, value: str):
        self.__city = value

    @property
    def province(self):
        return self.__province

    @province.setter
    def province(self, value: str):
        self.__province = value

    @property
    def special_instructions(self):
        return self.__special_instructions

    @special_instructions.setter
    def special_instructions(self, value: str):
        self.__special_instructions = value

    @property
    def business_type(self):
        return self.__business_type

    @business_type.setter
    def business_type(self, value: bool):
        self.__business_type = value

    @property
    def is_default_billing(self):
        return self.__is_default_billing

    @is_default_billing.setter
    def is_default_billing(self, value: bool):
        self.__is_default_billing = value

    @property
    def is_default_shipping(self):
        return self.__is_default_shipping

    @is_default_shipping.setter
    def is_default_shipping(self, value: bool):
        self.__is_default_shipping = value

    @property
    def address_hash(self) -> str:
        return self.__address_hash

    @address_hash.setter
    def address_hash(self, value: str):
        self.__address_hash = value

    def to_dict(self):
        return {
                'address_hash': self.__address_hash,
                'address_nickname': self.__address_nickname,
                'recipient_name': self.__recipient_name,
                'mobile_number': self.__mobile_number,
                'business_name': self.__business_name,
                'complex_building': self.__complex_building,
                'street_address': self.__street_address,
                'suburb': self.__suburb,
                'postal_code': self.__postal_code,
                'city': self.__city,
                'province': self.__province,
                'special_instructions': self.__special_instructions,
                'business_type': self.business_type,
                'is_default_billing': self.__is_default_billing,
                'is_default_shipping': self.__is_default_shipping
            }

class IdentificationNumber(object):
    MIN_LENGTH = 10
    MAX_LENGTH = 19

    def __init__(self, value: str):
        if not isinstance(value, str):
            raise ArgumentTypeException(self.__init__, 'value', value)

        value = value.strip()
        if (len(value) < self.__class__.MIN_LENGTH or len(value) > self.__class__.MAX_LENGTH):
            raise ArgumentValueException('The length of "{}" number must be between {} and {}'.format(
                self.__init__.__qualname__,
                self.__class__.MIN_LENGTH,
                self.__class__.MAX_LENGTH
            ))

        self.__value = value

    def __str__(self) -> str:
        return self.__value

    @property
    def value(self) -> str:
        return self.__value


class Information:
    def __init__(
        self,
        first_name: str,
        last_name: str,
        email: str,
        gender: str,
        addresses: List[dict],
        customer_id: str = None,
        identification_number: Optional[IdentificationNumber] = None
    ):
        if identification_number is not None and not isinstance(identification_number, IdentificationNumber):
            raise ArgumentTypeException(self.__init__, 'identification_number', identification_number)

        self.__first_name = first_name
        self.__last_name = last_name
        self.__email = email
        self.__gender = gender
        self.__addresses = [InformationAddress(**address) for address in addresses] if addresses is not None else []
        self.__customer_id = customer_id
        self.__identification_number = identification_number

    @property
    def identification_number(self) -> Optional[IdentificationNumber]:
        return self.__identification_number

    @identification_number.setter
    def identification_number(self, identification_number: Optional[IdentificationNumber]):
        if identification_number and not isinstance(identification_number, IdentificationNumber):
            raise ArgumentTypeException(self.identification_number, 'identification_number', identification_number)

        self.__identification_number = identification_number

    @property
    def addresses(self) -> List[InformationAddress]:
        return self.__addresses or []

    @addresses.setter
    def addresses(self, value: List[dict]):
        self.__addresses = [InformationAddress(**address) for address in value] if value is not None else []

    @property
    def email(self):
        return self.__email

    @email.setter
    def email(self, value):
        self.__email = value

    @property
    def first_name(self):
        return self.__first_name

    @first_name.setter
    def first_name(self, value):
        self.__first_name = value

    @property
    def last_name(self):
        return self.__last_name

    @last_name.setter
    def last_name(self, value):
        self.__last_name = value

    @property
    def gender(self):
        return self.__gender

    @gender.setter
    def gender(self, value):
        self.__gender = value

    @property
    def customer_id(self):
        return self.__customer_id

    @customer_id.setter
    def customer_id(self, value):
        self.__customer_id = value

    def to_dict(self):
        return {
            "email": self.__email,
            "first_name": self.__first_name,
            "last_name": self.__last_name,
            "gender": self.__gender,
            "addresses": [address.to_dict() for address in self.__addresses] if self.__addresses is not None else None,
            'identification_number': self.__identification_number.value if self.__identification_number else None,
        }


class InformationModel(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PROFILE#%s'
    INFORMATIONS_SK = 'USER_INFORMATIONS'

    __customer_id = None

    def __init__(self, customer_id: str):
        super(InformationModel, self).__init__(self.TABLE_NAME)
        # if customer_id is None:
        #     raise Exception("User is not authenticated.")
        self.__customer_id = customer_id

    def get_partition_key(self):
        return self.PARTITION_KEY % self.__customer_id

    def insert_item(self, info: Information) -> None:
        data = {
            'pk': self.get_partition_key(),
            'sk': self.INFORMATIONS_SK,
            'first_name': info.first_name,
            'last_name': info.last_name,
            'email': info.email,
            'gender': info.gender,
            'addresses': [address.to_dict() for address in info.addresses] if info.addresses is not None else None,
            'identification_number': info.identification_number.value if info.identification_number else None
        }

        self.table.put_item(Item=data)

    def add_information(self, info):
        result = self.table.update_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': self.INFORMATIONS_SK
            },
            UpdateExpression="SET " + ', '.join([
                'first_name = :first_name',
                'last_name = :last_name',
                'email = :email',
                'gender = :gender',
                'identification_number = :identification_number',
            ]),
            ExpressionAttributeValues={
                ':first_name': info['first_name'],
                ':last_name': info['last_name'],
                ':email': info['email'],
                ':gender': info['gender'],
                ':identification_number': info['identification_number'],
            },
            ReturnValues="UPDATED_NEW"
        )
        return result

    def get_item(self):
        instance = self.get_information()
        return instance.to_dict()

    def get_information(self) -> Information:
        item = super(InformationModel, self).get_item(self.INFORMATIONS_SK).get('Item', {})
        addresses = item.get('addresses', [])
        flag = False
        for address in addresses:
            if address.get('address_hash') is None:
                address['address_hash'] = hashlib.md5(str(address).encode('utf-8')).hexdigest()
                flag = True
        instance = Information(
            item.get('first_name'),
            item.get('last_name'),
            item.get('email'),
            item.get('gender'),
            addresses,
            self.__customer_id,
            IdentificationNumber(item.get('identification_number')) if item.get('identification_number') else None
        )
        if flag == True:
            self.insert_item(instance)
        return instance

    def add_address_attribute(self):
        result = self.table.update_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': self.INFORMATIONS_SK
            },
            UpdateExpression="SET addresses = :addresses",
            ExpressionAttributeValues={
                ':addresses': [],
            },
            ReturnValues="UPDATED_NEW"
        )
        return result

    def add_addresses(self, addresses):
        old_addresses = self.get_item()['addresses']

        if old_addresses is None:
            self.add_address_attribute()

        index = -1
        update_dict = {}
        add_list = []

        for k in range(len(addresses)):
            isExist = False
            address_hash = addresses[k].get('address_hash')
            if address_hash is None:
                addresses[k]['address_hash'] = hashlib.md5(str(addresses[k]).encode('utf-8')).hexdigest()
            for i in range(len(old_addresses)):
                if old_addresses[i]['address_hash'] == addresses[k]['address_hash']:
                    update_dict[str(i)]=k
                    isExist = True
                    break
            if isExist == False:
                add_list.append(addresses[k])
        if len(update_dict) > 0:
            updateExpression = "SET "
            expressionAttributeValues = {  
            }
            for index_in_old_addresses, index_in_addresses in update_dict.items():
                exp = "addresses[" + index_in_old_addresses + "] = :val"+ index_in_old_addresses + ", "
                updateExpression += exp
                expressionAttributeValues[':val'+index_in_old_addresses] = addresses[index_in_addresses]
            updateExpression = updateExpression[0:-2]

            result = self.table.update_item(
                Key={
                    'pk': self.get_partition_key(),
                    'sk': self.INFORMATIONS_SK
                },
                UpdateExpression=updateExpression,
                ExpressionAttributeValues=expressionAttributeValues,
                ReturnValues="UPDATED_NEW"
            )   
        if len(add_list) > 0:
            result = self.table.update_item(
                Key={
                    'pk': self.get_partition_key(),
                    'sk': self.INFORMATIONS_SK
                },
                UpdateExpression="SET addresses = list_append(addresses, :val)",
                ExpressionAttributeValues={
                    ':val': add_list
                },
                ReturnValues="UPDATED_NEW"
            )
        return result

    def add_address(self, address):
        address_hash = address.get('address_hash')
        if address_hash is None:
            address['address_hash'] = hashlib.md5(str(address).encode('utf-8')).hexdigest()
        old_addresses = self.get_item()['addresses']
        if old_addresses is None:
            self.add_address_attribute()
            old_addresses = []
        if len(old_addresses) == 0:
            address['is_default_billing']=True #if addresses is empty, new address should be default billing address.
            address['is_default_shipping']=True#if addresses is empty, new address should be default shipping address.
        index = -1
        for i in range(len(old_addresses)):
            if old_addresses[i]['address_hash'] == address['address_hash']:
                index = i
                break
        if index == -1:
            result = self.table.update_item(
                Key={
                    'pk': self.get_partition_key(),
                    'sk': self.INFORMATIONS_SK
                },
                UpdateExpression="SET addresses = list_append(addresses, :val)",
                ExpressionAttributeValues={
                    ':val': [address]
                },
                ReturnValues="UPDATED_NEW"
            )
            index = len(old_addresses)
        else:
            result = self.table.update_item(
                Key={
                    'pk': self.get_partition_key(),
                    'sk': self.INFORMATIONS_SK
                },
                UpdateExpression="SET addresses[" + str(index) + "] = :val",
                ExpressionAttributeValues={
                    ':val': address
                },
                ReturnValues="UPDATED_NEW"
            )
        if address['is_default_billing']:
            self.__reset_default_billing(index)
        if address['is_default_shipping']:
            self.__reset_default_shipping(index)

        return result  

    def get_address(self, address_hash):
        addresses = self.get_item()['addresses']
        for item in addresses:
            if item['address_hash'] == address_hash:
                return item
        raise Exception('There is no address.')

    def delete_address(self, address_hash):
        addresses = self.get_item()['addresses']
        for index in range(len(addresses)):
            if addresses[index]['address_hash'] == address_hash:
                query = "REMOVE addresses[%d]" % (index)
                result = self.table.update_item(
                    Key={
                        'pk': self.get_partition_key(),
                        'sk': self.INFORMATIONS_SK
                    },
                    UpdateExpression=query
                )
                if addresses[index]['is_default_billing']:
                    self.__reset_default_billing(0) #if default billing address was removed, the first address is set as default billing address
                if addresses[index]['is_default_shipping']:
                    self.__reset_default_shipping(0) #if default billing address was removed, the first address is set as default billing address
                return result
        raise Exception('There is no address.')

    def __reset_default_billing(self, index):
        addresses = self.get_item()['addresses']
        if addresses is None or len(addresses) <= index:
            return
        updateExpression = "SET "
        expressionAttributeValues = {  
        }
        for k in range(len(addresses)):
            if k != index:
                exp = "addresses[" + str(k) + "].is_default_billing = :false_value" + ", "
                updateExpression += exp
                expressionAttributeValues[':false_value'] = False
            else:
                exp = "addresses[" + str(k) + "].is_default_billing = :true_value" + ", "
                updateExpression += exp
                expressionAttributeValues[':true_value'] = True
        updateExpression = updateExpression[0:-2]

        self.table.update_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': self.INFORMATIONS_SK
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues="UPDATED_NEW"
        ) 
    
    def __reset_default_shipping(self, index):
        addresses = self.get_item()['addresses']
        if addresses is None or len(addresses) <= index:
            return
        updateExpression = "SET "
        expressionAttributeValues = {  
        }
        for k in range(len(addresses)):
            if k != index:
                exp = "addresses[" + str(k) + "].is_default_shipping = :false_value" + ", "
                updateExpression += exp
                expressionAttributeValues[':false_value'] = False
            else:
                exp = "addresses[" + str(k) + "].is_default_shipping = :true_value" + ", "
                updateExpression += exp
                expressionAttributeValues[':true_value'] = True
        updateExpression = updateExpression[0:-2]

        self.table.update_item(
            Key={
                'pk': self.get_partition_key(),
                'sk': self.INFORMATIONS_SK
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues="UPDATED_NEW"
        ) 

class InformationService(Base):
    def __init__(self):
        Base.__init__(self)
        self.table = self.dynamodb.Table(settings.AWS_DYNAMODB_CMS_TABLE_NAME)
    
    def get(self, email):
        if not email:
            raise ValueError('email is empty') 
        filterExpression =  "sk = :skValue AND email = :emailValue"
        expressionAttributeValues = {
            ":skValue": "USER_INFORMATIONS",
            ":emailValue": email
        }
        response = self.table.scan(
            FilterExpression = filterExpression,
            ExpressionAttributeValues = expressionAttributeValues
        )
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.table.scan(
                FilterExpression = filterExpression,
                ExpressionAttributeValues = expressionAttributeValues,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            data.extend(response['Items'])

        if len(data) == 0:
            raise Exception('There is no items') 
        pk = str(data[0].get('pk', ''))
        customer_id = pk.replace('PROFILE#', '')
        if not customer_id:
            raise Exception('customer_id is invalid') 
        information_model = InformationModel(customer_id)
        return information_model


