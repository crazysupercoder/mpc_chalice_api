from boto3.dynamodb.conditions import Key
from typing import List, Tuple
from chalicelib.settings import settings
from ..base import DynamoModel, boto3
from .preferences import Preference
from .Informations import InformationModel, Information, IdentificationNumber
from .UserQuestions import UserQuestionModel, USER_QUESTION_TYPE
from ..product_tracking import ProductsTrackingModel
from ...mpc.categories import Category, CategoryEntry
from ...mpc.product_sizes import SizeOptions, Gender
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface, SqsSenderImplementation
from chalicelib.libs.purchase.core.values import Id
from chalicelib.libs.purchase.core.customer import CustomerTier
from chalicelib.libs.purchase.customer.storage import CustomerStorageImplementation, CustomerTierStorageImplementation

user_question_model = UserQuestionModel()

class PROFILE_SAVE_MODE:
    profile = 'p'
    session = 's'


class CommunicationPreferencesUpdateSqsSenderEvent(SqsSenderEventInterface):
    def __init__(self, email, alert_for_favorite_brand, daily_email, sms, weekly_email):
        self.__email = email
        self.__alert_for_favorite_brand = alert_for_favorite_brand
        self.__daily_email = daily_email
        self.__sms = sms
        self.__weekly_email = weekly_email

    @classmethod
    def _get_event_type(cls) -> str:
        return 'communication_preferences'

    @property
    def event_data(self) -> dict:
        return {
            'email': self.__email,
            'alert_for_favorite_brand': self.__alert_for_favorite_brand,
            'daily_email': self.__daily_email,
            'sms': self.__sms,
            'weekly_email': self.__weekly_email,
        }


class Profile(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'PROFILE#%s'

    AUTH_USER_PK = 'PROFILE#%s'
    GUEST_USER_PK = 'GUEST#%s'

    QUESTIONS_SK_PREFIX = 'QUESTIONS#'
    GUEST_USER_ATTR_SK = 'USER_ATTRIBUTES'
    PRODUCT_ATTR_SK = 'PRODUCT_ATTRIBUTES'
    PREFERENCES_SK = 'USER_PREFERENCES'

    FAVORITE_ITEMS_SK = 'USER_FAVORITES'
    FAVORITE_CATEGORIES_SK = 'USER_CATEGORIES'
    __SCORED_PRODUCTS_ATTR_NAME__ = 'is_scored_products'

    __tracking_data = None
    __user_attributes: dict = None

    def __init__(self, session_id, customer_id=None, email=None):
        super(Profile, self).__init__(self.TABLE_NAME)
        self.__is_anonymous = customer_id is None
        self.__session_id = session_id
        self.__customer_id = customer_id
        self.__email = email
        self.__purchase_customer_tier_lazy_loading_cache = None

    @property
    def user_attributes(self) -> dict:
        if not self.__user_attributes:
            self.__user_attributes = self.get_user_attributes()
        return self.__user_attributes

    @property
    def is_anonymous(self):
        return self.__is_anonymous

    @is_anonymous.setter
    def is_anonymous(self, value):
        self.__is_anonymous = value

    @property
    def is_personalized(self) -> bool:
        return self.user_attributes.get(self.__SCORED_PRODUCTS_ATTR_NAME__, False)

    @is_personalized.setter
    def is_personalized(self, value: bool):
        self.set_user_attribute(self.__SCORED_PRODUCTS_ATTR_NAME__, bool(value))

    def get_partition_key(self, mode=None, **kwargs):
        if mode == PROFILE_SAVE_MODE.profile:
            return self.AUTH_USER_PK % self.customer_id
        elif mode == PROFILE_SAVE_MODE.session:
            return self.GUEST_USER_PK % self.session_id
        else:
            if self.is_anonymous:
                return self.GUEST_USER_PK % self.session_id
            else:
                return self.AUTH_USER_PK % self.customer_id

    @property
    def session_id(self):
        return self.__session_id

    @session_id.setter
    def session_id(self, value):
        self.__session_id = value

    @property
    def customer_id(self):
        return self.__customer_id

    @customer_id.setter
    def customer_id(self, value):
        self.__customer_id = value

    @property
    def email(self) -> str:
        return self.__email

    @email.setter
    def email(self, value: str):
        self.__email = value

    @property
    def tracking_data(self) -> dict:
        if self.__tracking_data is None:
            model = ProductsTrackingModel()
            self.__tracking_data = model.get_visited_products_aggregation_data(self.customer_id)
        return self.__tracking_data  # if isinstance(self.__tracking_data, list) else []

    @property
    def gender(self):
        return self.get_gender()

    @property
    def name(self):
        return self.get_name()

    @property
    def brands(self) -> List[str]:
        return self.get_brands()

    @property
    def product_types(self):
        return self.get_product_types()

    @property
    def sizes(self) -> SizeOptions:
        return self.get_sizes()

    @sizes.setter
    def sizes(self, sizes: dict):
        sizes = SizeOptions(data=sizes)
        self.set_product_attribute('sizes', sizes.to_dict())

    @property
    def preference(self) -> Preference:
        item = self.get_item(self.PREFERENCES_SK).get('Item')
        if item is None:
            return Preference()
        else:
            return Preference(**item.get('data', {}))

    @preference.setter
    def preference(self, value: dict):
        item = self.get_item(self.PREFERENCES_SK).get('Item')
        if item is None:
            preference = Preference(**value)
            self.insert_item(self.PREFERENCES_SK, {'data': preference.to_dict()})
        else:
            preference = Preference(**item.get('data'))
            for key, value in value.items():
                if hasattr(preference, key):
                    setattr(preference, key, value)

            self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': self.PREFERENCES_SK,
            }, AttributeUpdates={
                'data': {'Value': preference.to_dict()}
            })

        try:
            event = CommunicationPreferencesUpdateSqsSenderEvent(
                self.email,
                preference.alert_for_favorite_brand,
                preference.daily_email,
                preference.sms,
                preference.weekly_email
            )
            SqsSenderImplementation().send(event)
        except Exception as e:
            print(str(e))
            return False

    @property
    def tier(self) -> dict:
        # customer tier - is a part of purchase module, not account information

        def __tier_to_dict(customer_tier: CustomerTier) -> dict:
            return {
                'name': customer_tier.name.value,
                'discount_rate': customer_tier.credit_back_percent.value,
                'is_neutral': customer_tier.is_neutral
            }

        # cache
        if self.__purchase_customer_tier_lazy_loading_cache:
            return __tier_to_dict(self.__purchase_customer_tier_lazy_loading_cache)

        # guests are in neutral tier
        if self.is_anonymous:
            self.__purchase_customer_tier_lazy_loading_cache = CustomerTierStorageImplementation().get_neutral()
            return __tier_to_dict(self.__purchase_customer_tier_lazy_loading_cache)

        # get assigned customer tier
        customer = CustomerStorageImplementation().load(Id(self.customer_id))
        self.__purchase_customer_tier_lazy_loading_cache = customer.tier
        return __tier_to_dict(self.__purchase_customer_tier_lazy_loading_cache)

    def get_user_attributes(self):
        item = self.get_item(self.GUEST_USER_ATTR_SK)
        return item.get('Item', {})

    def get_product_attributes(self):
        item = self.get_item(self.PRODUCT_ATTR_SK)
        return item.get('Item')

    def get_user_attribute(self, attr_name):
        return self.user_attributes.get(attr_name)

    def get_product_attribute(
            self, attr_name: str, default: dict=[]):
        attributes = self.get_product_attributes()
        return default if attributes is None else attributes.get(attr_name, default)

    def get_gender(self):
        return self.get_user_attribute('gender')

    def get_name(self):
        return self.get_user_attribute('name')

    def get_sizes(self) -> SizeOptions:
        sizes = self.get_product_attribute('sizes', default=None)
        if sizes is None:
            sizes = SizeOptions(genders=[
                Gender.women
                if self.gender is None
                else self.gender])
        else:
            sizes = SizeOptions(data=sizes)
        return sizes

    def get_brands(self) -> List[str]:
        brands = self.get_product_attribute('brands')
        # if not self.is_anonymous and len(brands) == 0:
        #     # TODO: Getting brands from personalize to save them by default
        #     product_model = Product()
        #     brands = product_model.get_top_brands(customer_id=self.email, size=2)
        #     brand_names = [brand.get('brand_name') for brand in brands]
        #     self.set_brands(brand_names)
        #     return brand_names
        # else:
        return brands

    def get_product_types(self):
        return self.get_product_attribute('product_types')

    def set_user_attribute(self, attr_name, value):
        item = self.get_user_attributes()

        if item is None:
            return self.insert_item(self.GUEST_USER_ATTR_SK, {attr_name: value})
        else:
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.GUEST_USER_ATTR_SK
                }, AttributeUpdates={
                    attr_name: {'Value': value}
                })
                return True
            except Exception as e:
                print(str(e))
                return False

    def set_product_attributes(self, **kwargs):
        item = self.get_product_attributes()
        params = dict()
        for key, value in kwargs.items():
            params[key] = value

        if item is None:
            return self.insert_item(self.PRODUCT_ATTR_SK, params)
        else:
            for key, value in params.items():
                params.update({
                    key: {'Value': value}
                })
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.PRODUCT_ATTR_SK
                }, AttributeUpdates=params)
                return True
            except Exception as e:
                print(str(e))
                return False

    def set_product_attribute(self, attr_name, value):
        item = self.get_product_attributes()

        if item is None:
            return self.insert_item(self.PRODUCT_ATTR_SK, {attr_name: value})
        else:
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.PRODUCT_ATTR_SK
                }, AttributeUpdates={
                    attr_name: {'Value': value}
                })
                return True
            except Exception as e:
                print(str(e))
                return False

    def set_name(self, name):
        return self.set_user_attribute('name', name)

    def set_gender(self, gender):
        return self.set_user_attribute('gender', gender)

    def set_brands(self, brands):
        return self.set_product_attribute('brands', brands)

    def add_brand(self, brand_name: str):
        brands = self.brands
        if brand_name.lower() not in [item.lower() for item in brands]:
            brands.append(brand_name)
            return self.set_brands(brands)
        else:
            return False

    def set_favorite(self, config_sku: str, like: bool=True):
        item = self.get_item(self.FAVORITE_ITEMS_SK).get('Item')
        if item is None:
            likes = [config_sku] if like else []
            dislikes = [] if like else [config_sku]
            return self.insert_item(self.FAVORITE_ITEMS_SK, {
                'likes': likes,
                'dislikes': dislikes,
            })
        else:
            likes = item.get('likes', [])
            dislikes = item.get('dislikes', [])

            if like:
                if config_sku in dislikes:
                    dislikes.remove(config_sku)
                if config_sku not in likes:
                    likes.append(config_sku)
            else:
                if config_sku in likes:
                    likes.remove(config_sku)
                if config_sku not in dislikes:
                    dislikes.append(config_sku)
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.FAVORITE_ITEMS_SK
                }, AttributeUpdates={
                    'likes': {'Value': likes},
                    'dislikes': {'Value': dislikes}
                })
                return True
            except Exception as e:
                print(str(e))
                return False

    def set_dislike(self, config_sku: str):
        return self.set_favorite(config_sku, like=False)

    def set_like(self, config_sku: str):
        return self.set_favorite(config_sku, like=True)

    @property
    def likes(self) -> List[str]:
        item = self.get_item(self.FAVORITE_ITEMS_SK)
        return item['Item'].get('likes', []) if item.get('Item') else []

    @property
    def dislikes(self) -> List[str]:
        item = self.get_item(self.FAVORITE_ITEMS_SK)
        return item['Item'].get('dislikes', []) if item.get('Item') else []

    @property
    def categories(self) -> List[CategoryEntry]:
        categories, exists = self.get_categories()
        return categories

    def remove_brand(self, brand_name: str):
        brands = self.brands
        if brand_name.lower() in [item.lower() for item in brands]:
            candidates = [
                item for item in [
                    brand for brand in brands
                    if brand_name.lower() == brand.lower()
                ]
            ]

            brands.remove(candidates[0])
            return self.set_brands(brands)
        else:
            return False

    def add_category(self, category_id: int):
        category_model = Category()
        categories, exists = self.get_categories()
        category = category_model.get_item_v2(category_id)
        if category is None or\
                len([item for item in categories if item.id == category.id]) > 0:
            return False

        categories.append(category)
        if exists:
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.FAVORITE_CATEGORIES_SK,
                }, AttributeUpdates={
                    'data': {'Value': [item.to_dict() for item in categories]}
                })
                return True
            except Exception as e:
                print(str(e))
                return False
        else:
            return self.insert_item(
                self.FAVORITE_CATEGORIES_SK,
                {
                    "data": [item.to_dict() for item in categories]
                })

    def remove_category(self, category_id: int):
        category_model = Category()
        categories, _ = self.get_categories()
        category = category_model.get_item_v2(category_id)
        if category is None or\
                len([item for item in categories if item.id == category.id]) == 0:
            return False
        else:
            categories = [item for item in categories if item.id != category.id]
            try:
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.FAVORITE_CATEGORIES_SK,
                }, AttributeUpdates={
                    'data': {'Value': [item.to_dict() for item in categories]}
                })
                return True
            except Exception as e:
                print(str(e))
                return False

    def get_categories(self) -> Tuple[List[CategoryEntry], bool]:
        item = self.get_item(self.FAVORITE_CATEGORIES_SK).get('Item', None)
        exists = (item is not None)
        items = item.get('data', []) if exists else []

        categories_ids = [int(item.get('category_id')) for item in items]
        categories = Category().get_by_ids(tuple(categories_ids))
        categories = list(categories)

        return categories, exists

    def set_product_types(self, product_types):
        return self.set_product_attribute('product_types', product_types)

    def sync_user_data(self, **kwargs):
        if self.is_anonymous:
            return False

        # Getting data saved in session space
        items = self.table.query(
            KeyConditionExpression=Key('pk').eq(self.get_partition_key(mode=PROFILE_SAVE_MODE.session)) &
            Key('sk').begins_with(self.QUESTIONS_SK_PREFIX)).get('Items', [])
        for answer in items:
            self.save_answer(answer['sk'].split('#')[-1], answer['data'])
            # TODO: Should delete this?

        # TODO: Sync product attributes guest to user
        item = self.table.get_item(Key={
            'pk': self.get_partition_key(mode=PROFILE_SAVE_MODE.session),
            'sk': self.PRODUCT_ATTR_SK
        }).get('Item')

        if item is not None:
            item.pop('pk')
            item.pop('sk')
            self.set_product_attributes(**item)

        # TODO: Sync user attributes guest to user
        item = self.table.get_item(Key={
            'pk': self.get_partition_key(mode=PROFILE_SAVE_MODE.session),
            'sk': self.GUEST_USER_ATTR_SK
        }).get('Item')

        if item is None:
            return {}
        else:
            item.pop('pk')
            item.pop('sk')
            return item

    @property
    def informations(self):
        information_model = InformationModel(self.customer_id)
        information = information_model.get_item()
        return information

    @informations.setter
    def informations(self, value: dict):
        information_model = InformationModel(self.customer_id)
        information = Information(
            value['first_name'],
            value['last_name'],
            value['email'],
            value['gender'],
            value['addresses'],
            self.customer_id,
            IdentificationNumber(value.get('identification_number')) if value.get('identification_number') else None
        )
        information_model.insert_item(information)

    def add_information(self, info):
        information_model = InformationModel(self.customer_id)
        return information_model.add_information(info)

    def add_addresses(self, addresses):
        information_model = InformationModel(self.customer_id)
        if isinstance(addresses, list): 
            return information_model.add_addresses(addresses)
        else:
            return information_model.add_addresses([addresses])

    def add_address(self, address):
        information_model = InformationModel(self.customer_id)
        return information_model.add_address(address)

    def get_address(self, address_nickname):
        information_model = InformationModel(self.customer_id)
        return information_model.get_address(address_nickname)

    def delete_address(self, address_nickname):
        information_model = InformationModel(self.customer_id)
        return information_model.delete_address(address_nickname)

    @property
    def questions(self):
        items = self.table.query(
        KeyConditionExpression=Key('pk').eq(self.get_partition_key()) &
        Key('sk').begins_with(self.QUESTIONS_SK_PREFIX)).get('Items')
        if len(items) == 0:
            self.add_name_question()
        items = self.table.query(
        KeyConditionExpression=Key('pk').eq(self.get_partition_key()) &
        Key('sk').begins_with(self.QUESTIONS_SK_PREFIX)).get('Items')
        return [item['data'] for item in items]

    # @property
    # def answers(self) -> List[dict]:
    #     response = self.table.query(
    #         KeyConditionExpression=Key('pk').eq(self.get_partition_key()) & Key('sk').begins_with(self.QUESTIONS_SK_PREFIX)
    #     )
    #     items = response.get('Items', [])
    #     return items

    # @classmethod
    # def get_answers_by_customer(cls, customer_id: str) -> List[dict]:
    #     dynamodb = boto3.resource('dynamodb', region_name=cls.AWS_REGION)
    #     table = dynamodb.Table(cls.TABLE_NAME)
    #     response = table.query(
    #         KeyConditionExpression=Key('pk').eq(
    #             cls.PARTITION_KEY % customer_id) & Key('sk').begins_with(
    #                 cls.QUESTIONS_SK_PREFIX)
    #     )
    #     items = response.get('Items', [])
    #     return items

    def get_question(self, number):
        items = self.table.query(
        KeyConditionExpression=Key('pk').eq(self.get_partition_key()) &
        Key('sk').eq(self.QUESTIONS_SK_PREFIX +  str(number))).get('Items')
        return items[0]['data'] if len(items) > 0 else None

    def manage_answer(self, answer):
        if answer['attribute']['type'] == USER_QUESTION_TYPE.customer:
            # TODO: manage customers
            pass
        elif answer['attribute']['type'] == USER_QUESTION_TYPE.product:
            pass

    def save_answer(self, number, answer):
        item = self.get_question(number)

        try:
            item['answer'] = answer
            self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': self.QUESTIONS_SK_PREFIX +  str(number),
            }, AttributeUpdates={
                'data': {'Value': item}
            })
            return True
        except Exception as e:
            print(str(e))
            return False

    def add_question(self, name, question):
        count = len(self.questions)
        if name:
            temp = question['question'].replace('{name}', name)
            question['question'] = temp
        question['number'] = str(count + 1)
        try:
            self.table.update_item(Key={
                'pk': self.get_partition_key(),
                'sk': self.QUESTIONS_SK_PREFIX +  str(count + 1),
            }, AttributeUpdates={
                'data': {'Value': question}
            })
            return True
        except Exception as e:
            print(str(e))
            return False

    def add_name_question(self):
        questions = user_question_model.get_all(convert = False)
        for question in questions:
            if question.attribute_value == 'name':
                temp = user_question_model.get_item(question.id)
                temp['number'] = '1'
                self.table.update_item(Key={
                    'pk': self.get_partition_key(),
                    'sk': self.QUESTIONS_SK_PREFIX + '1',
                }, AttributeUpdates={
                    'data': {'Value': temp}
                })
                return True
        return False
    
    def add_brand_category_size_questions(self, names):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        i = 0
        print(names)
        print(len(names))
        while i < len(names):
            for question in questions:
                if question.attribute_value == 'brand':
                    flag = True
                    for old in old_questions:
                        if old['attribute']['value'] == 'brand' and names[i] in old['question']:
                            flag = False
                            break
                    if flag == True:
                        temp = user_question_model.get_item(question.id)
                        self.add_question(names[i], temp)
                elif question.attribute_value == 'category':
                    flag = True
                    for old in old_questions:
                        if old['attribute']['value'] == 'category' and names[i] in old['question']:
                            flag = False
                            break
                    if flag == True:
                        temp = user_question_model.get_item(question.id)
                        self.add_question(names[i], temp)
                elif question.attribute_value == 'size':
                    flag = True
                    for old in old_questions:
                        if old['attribute']['value'] == 'size' and names[i] in old['question']:
                            flag = False
                            break
                    if flag == True:
                        temp = user_question_model.get_item(question.id)
                        self.add_question(names[i], temp)
            if i < len(names) - 1:    
                temp = {
                    'question': 'Do you want to set preferences for {name} ?',
                    'attribute': {
                        'type': 'customer',
                        'value': 'preferences_shop4_other',
                    },
                    'options':[
                        'Yes - Let\'s add {name}', 'No - Skip for now'
                    ]
                }
                flag = True
                for old in old_questions:
                    if names[i + 1] in old['question']:
                        flag = False
                        break  
                if flag == True:
                    self.add_question(names[i + 1], temp)
            else:
                temp = {
                    'question': 'Would you like to add preferences for another person?',
                    'attribute': {
                        'type': 'customer',
                        'value': 'preferences_shop4_another',
                    },
                    'options':[
                        'Yes - Let\'s add someone', 'No - I \'ve done'
                    ]
                }
                flag = True
                for old in old_questions:
                    if old['attribute']['value'] == 'preferences_shop4_another':
                        flag = False
                        break  
                if flag == True:
                    self.add_question('', temp)       
            i+=1

    def add_size_question(self, name):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'size':
                for old in old_questions:
                    if old['attribute']['value'] == 'size' and name in old['question']:
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question(name, temp)
                return

    def add_brand_question(self, name):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'brand':
                for old in old_questions:
                    if old['attribute']['value'] == 'brand' and name in old['question']:
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question(name, temp)
                return

    def add_category_question(self, name):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'category':
                for old in old_questions:
                    if old['attribute']['value'] == 'category' and name in old['question']:
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question(name, temp)
                return    
    
    def add_shop4_question(self, name):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'shop4':
                for old in old_questions:
                    if old['attribute']['value'] == 'shop4':
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question(name, temp)
                return  

    def add_language_question(self):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'languages':
                for old in old_questions:
                    if old['attribute']['value'] == 'languages':
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question('', temp)
                return  

    def add_gender_question(self):
        questions = user_question_model.get_all(convert = False)
        old_questions = self.questions
        for question in questions:
            if question.attribute_value == 'gender':
                for old in old_questions:
                    if old['attribute']['value'] == 'gender':
                        return
                temp = user_question_model.get_item(question.id)
                self.add_question('', temp)
                return  

    def add_save_preferences_question(self):
        old_questions = self.questions
        question = {
            'question': 'Let\'s save your preferences:',
            'attribute': {
                'type': 'customer',
                'value': 'save_preferences',
            },
            'options': []
        }
        for old in old_questions:
            if old['attribute']['value'] == 'save_preferences':
                return
        self.add_question('', question)

    def add_names_shop4_question(self, shop4list):
        old_questions = self.questions
        if "Just me" in shop4list: shop4list.remove("Just me")
        question = {
            'question': 'Please add the names of those you want to shop for:',
            'attribute': {
                'type': 'customer',
                'value': 'names_shop4',
            },
            'options': [
                {
                    'value': 'Who do you shop for in ' + item + '?',
                    'png_image': None
                } for item in shop4list
            ]
        }
        for old in old_questions:
            if old['attribute']['value'] == 'names_shop4':
                return
        self.add_question('', question)
    
    def add_preferences_shop4_other_question(self, name):
        old_questions = self.questions
        if name == 'the others you shop for':
            answer_tail = 'them'
        else:
            answer_tail = name
        question = {
            'question': 'Do you want to set preferences for ' + name + '?',
            'attribute': {
                'type': 'customer',
                'value': 'preferences_shop4_other',
            },
            'options':[
                'Yes - Let\'s add ' + answer_tail, 'No - Skip for now'
            ]
        }
        for old in old_questions:
            if name in old['question']:
                return
        self.add_question('', question)
    
    def add_preferences_shop4_another_question(self):
        old_questions = self.questions
        question = {
            'question': 'Would you like to add preferences for another person?',
            'attribute': {
                'type': 'customer',
                'value': 'preferences_shop4_another',
            },
            'options':[
                'Yes - Let\'s add someone', 'No - I \'ve done'
            ]
        }
        for old in old_questions:
            if old['attribute']['value'] == 'preferences_shop4_another':
                return
        self.add_question('', question)
