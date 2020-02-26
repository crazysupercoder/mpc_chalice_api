import os
import boto3
import random
import json
from datetime import datetime
from collections import OrderedDict
from ...settings import settings
from ..models.mpc.brands import Brand
from ..models.mpc.product_types import ProductType
from ..models.mpc.product_sizes import ProductSize


brand_model = Brand()
product_type_model = ProductType()


class PersonalizeBase(object):
    PERSONALIZE_CAMPAIGN_ARN = None
    AWS_REGION = settings.AWS_PERSONALIZE_DEFAULT_REGION
    DEFAULT_SOLUTION_VERSION_ARN = None
    SIMILAR_ITEMS_CAMPAIGN_ARN = None
    PERSONALIZED_RANKING_CAMPAIGN_ARN = None
    TRACKING_ID = None

    @classmethod
    def get_recommends(cls, customer_id, size=6, **kwargs):
        return cls.get_recommend_ids(customer_id, size, **kwargs)

    @classmethod
    def get_recommend_ids(cls, customer_id='BLANK', size=None, **kwargs):
        if not customer_id:
            customer_id = 'BLANK'
        if kwargs.get('region') is not None:
            cls.AWS_REGION = kwargs.get('region')
        session = boto3.Session(region_name=cls.AWS_REGION)
        personalizeRt = session.client('personalize-runtime')
        if cls.PERSONALIZE_CAMPAIGN_ARN is None:
            raise NotImplementedError('CAMPIGN_ARN is required.')

        try:
            if size is None:
                size = 200
            response = personalizeRt.get_recommendations(
                campaignArn=cls.PERSONALIZE_CAMPAIGN_ARN,
                userId=customer_id, numResults=size)
                
            return response
        except Exception as e:
            print(str(e))
            return {'itemList': []}

    @classmethod
    def get_metrics(cls):
        return cls.get_solution_metrics(cls.DEFAULT_SOLUTION_VERSION_ARN)

    @classmethod
    def get_solution_metrics(cls, solution_arn):
        session = boto3.Session(region_name=cls.AWS_REGION)
        personalize = session.client('personalize')
        response = personalize.get_solution_metrics(
            solutionVersionArn = solution_arn)
        return response['metrics']

    @classmethod
    def get_similar_items(cls, item_id, customer_id='BLANK', size=10):
        session = boto3.Session(region_name=cls.AWS_REGION)
        personalizeRt = session.client('personalize-runtime')
        if cls.SIMILAR_ITEMS_CAMPAIGN_ARN is None:
            raise NotImplementedError('SIMILAR_ITEMS_CAMPAIGN_ARN is required.')

        response = personalizeRt.get_recommendations(
            campaignArn=cls.SIMILAR_ITEMS_CAMPAIGN_ARN, userId=customer_id, itemId=item_id)

        product_type_ids = [item['itemId'] for item in response['itemList'][:size] if item['itemId'] != '']
        product_type_model = ProductType()
        product_types = product_type_model.filter_by_product_type_ids(product_type_ids)
        result = sorted(product_types['Items'], key=lambda k: product_type_ids.index(k['sk']))
        return cls.convert(result)

    @classmethod
    def get_personalized_ranking(cls, item_ids, customer_id='BLANK', user_defined=[], **kwargs):
        user_defined = [str(int(item)) for item in user_defined]
        session = boto3.Session(region_name=cls.AWS_REGION)
        personalizeRt = session.client('personalize-runtime')
        if cls.PERSONALIZED_RANKING_CAMPAIGN_ARN is None:
            raise NotImplementedError('PERSONALIZED_RANKING_CAMPAIGN_ARN is required.')
        
        if len(item_ids) > 0:
            response = personalizeRt.get_personalized_ranking(
                campaignArn=cls.PERSONALIZED_RANKING_CAMPAIGN_ARN, userId=customer_id, inputList=item_ids)

            items = [item['itemId'] for item in response['personalizedRanking'] if item['itemId'] != '']
            items = [item for item in items if item not in user_defined]
        else:
            items = []
        return user_defined + items

    
    @classmethod
    def convert(cls, items):
        return items

    @classmethod
    def track_event(cls, customer_id, session_id, item_id, event_type='click', value=''):
        if cls.TRACKING_ID is None:
            raise NotImplementedError('TRACKING_ID is required.')

        session = boto3.Session(region_name=cls.AWS_REGION)
        client = session.client('personalize-events')
        response = client.put_events(
            trackingId=cls.TRACKING_ID,
            userId=customer_id,
            sessionId=session_id,
            eventList=[
                {
                    'eventType': event_type,
                    'properties': json.dumps({
                        'event': event_type,
                        'itemId': str(item_id),
                        'eventValue': value
                    }),
                    'sentAt': datetime.now()
                },
            ]
        )
        return response


class SimpleSkuPersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_SIMPLE_SKU_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_SIMPLE_SKU_RECOMMEND_CAMPAIGN_ARN


class ConfigSkuPersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_CONFIG_SKU_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_CAMPAIGN_ARN
    DEFAULT_SOLUTION_VERSION_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_SOLUTION_VERSION_ARN

    SIMILAR_ITEMS_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_CAMPAIGN_ARN
    SIMILAR_ITEMS_SOLUTION_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_SOLUTION_VERSION_ARN
    PERSONALIZED_RANKING_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_RANKING_CAMPAIGN_ARN
    PERSONALIZED_RANKING_SOLUTINO_ARN = settings.AWS_PERSONALIZE_CONFIG_SKU_RANKING_SOLUTION_VERSION_ARN
    TRACKING_ID = settings.AWS_PERSONALIZE_CONFIG_SKU_TRACKING_ID

    @classmethod
    def get_recommends(cls, customer_id='BLANK', size=None, **kwargs):
        response = cls.get_recommend_ids(customer_id=customer_id, size=size)
        ids = [item['itemId'] for item in response.get('itemList', [])]
        return ids

    @classmethod
    def get_similar_items_solution_metrics(cls):
        return cls.get_solution_metrics(cls.SIMILAR_ITEMS_SOLUTION_ARN)

    @classmethod
    def get_personalized_ranking_solution_metrics(cls):
        return cls.get_solution_metrics(cls.PERSONALIZED_RANKING_SOLUTINO_ARN)


class ProductSizePersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_SIZE_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_SIZE_RECOMMEND_CAMPAIGN_ARN
    DEFAULT_SOLUTION_VERSION_ARN = settings.AWS_PERSONALIZE_SIZE_RECOMMEND_SOLUTION_VERSION_ARN
    PERSONALIZED_RANKING_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_SIZE_RANKING_CAMPAIGN_ARN
    TRACKING_ID = settings.AWS_PERSONALIZE_SIZE_TRACKING_ID

    @classmethod
    def get_recommends(cls, customer_id='BLANK', size=6, **kwargs):
        product_size_model = ProductSize()
        response = cls.get_recommend_ids(customer_id=customer_id, size=size)
        size_ids = [item['itemId'] for item in response['itemList']]
        product_sizes = product_size_model.filter_by_product_size_ids(size_ids)
        result = sorted(product_sizes, key=lambda k: size_ids.index(k['sk']))
        response = [{
                'id': int(item['product_size_id']),
                'name': item['product_size_name']
            } for item in result]
        return response

    @classmethod
    def get_top_size(cls, size_names, customer_id='BLANK', **kwargs):
        product_size_model = ProductSize()
        size_ids = product_size_model.filter_by_product_size_name(size_names)
        ranked_ids = cls.get_personalized_ranking(size_ids, customer_id=customer_id)
        items = product_size_model.filter_by_product_size_ids(ranked_ids)
        items = sorted(items, key=lambda k: ranked_ids.index(k['sk']))
        if len(items) > 0:
            return items[0].get('product_size_name')
        else:
            return None


class ProductBrandPersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_BRAND_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_BRAND_RECOMMEND_CAMPAIGN_ARN
    DEFAULT_SOLUTION_VERSION_ARN = settings.AWS_PERSONALIZE_BRAND_RECOMMEND_SOLUTION_VERSION_ARN
    TRACKING_ID = settings.AWS_PERSONALIZE_BRAND_TRACKING_ID

    @classmethod
    def get_recommends(
            cls, customer_id='BLANK', size=6,
            exclude=[], user_defined=[], **kwargs):
        user_defined = [item.lower() for item in user_defined]
        recommend_result = cls.get_recommend_ids(customer_id, **kwargs)
        brand_names = [
            item['itemId'].lower()
            for item in recommend_result['itemList']
            if item['itemId'] != '' and item['itemId'].lower() not in exclude][:size]

        brand_names = [brand for brand in brand_names if brand not in user_defined]
        brand_names = user_defined + brand_names
        brand_items = brand_model.filter_by_brand_names(brand_names)["Items"]
        result = sorted(brand_items, key=lambda k: brand_names.index(k['brand_name']))
        brands = [{
            'id': item['sk'],
            'name': item['brand_name'],
            'brand_name': item['brand'],
            'new': False,
            'favorite': random.choice([True, False]),
            'image': {
                'title': item['brand'],
                'src': item['logo_url'] if item.get('logo_url') else 'https://placeimg.com/155/136/arch',
            }
        } for item in result]
        return brands


class ProductTypePersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_PRODUCT_TYPE_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_CAMPAIGN_ARN
    DEFAULT_SOLUTION_VERSION_ARN = settings.AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_SOLUTION_VERSION_ARN
    SIMILAR_ITEMS_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_PRODUCT_TYPE_SIMILAR_ITEMS_CAMPAIGN_ARN
    PERSONALIZED_RANKING_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_PRODUCT_TYPE_RANKING_CAMPAIGN_ARN
    TRACKING_ID = settings.AWS_PERSONALIZE_PRODUCT_TYPE_TRACKING_ID

    @classmethod
    def convert(cls, items):
        return [{
            'id': int(item['product_type_id']),
            'name': item['product_type_name'],
            'gender': item.get('product_gender'),
            'image': {
                'src': item['image'], 'title': item['product_type_name']
            }
        } for item in items]

    @classmethod
    def get_recommends(cls, customer_id='BLANK', size=6, user_defined=[], **kwargs):
        recommend_result = cls.get_recommend_ids(customer_id, size, **kwargs)
        product_type_ids = [item['itemId'] for item in recommend_result['itemList'] if item['itemId'] != ''][:size]
        product_type_ids = [item for item in product_type_ids if item not in user_defined]
        product_type_ids = user_defined + product_type_ids
        product_type_items = product_type_model.filter_by_product_type_ids(product_type_ids)["Items"]
        result = sorted(product_type_items, key=lambda k: product_type_ids.index(str(k['product_type_id'])))
        return cls.convert(result)

    @classmethod
    def get_subtypes(cls, customer_id='BLANK', gender=None, size=6, product_type_name=None, **kwargs):
        if product_type_name is None:
            product_type_id = cls.get_recommend_ids(customer_id=customer_id, size=1).get('itemList')[0]['itemId']
        else:
            product_types = product_type_model.filter_by_product_type_name([product_type_name])
            if len(product_types) == 0:
                product_type_id = cls.get_recommend_ids(customer_id=customer_id, size=1).get('itemList')[0]['itemId']
            else:
                product_type_id = product_types[0]['sk']

        product_type = product_type_model.find_by_id(product_type_id)
        subtypes = product_type_model.get_child_product_types(product_type_id, gender=gender, check_stock=True)
        # TODO: The current personalize engine didn't consider subtypes
        # if len(subtypes) > 1:
        #     product_type_ids = cls.get_personalized_ranking([item['sk'] for item in subtypes])
        #     sorted_subtypes = sorted(subtypes, key=lambda x: product_type_ids.index(x['sk']))[:size]
        # else:
        #     sorted_subtypes = []
        return {
            'product_type': product_type,
            'sub_types': subtypes
        }


class ProductPricePersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_PRICE_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_PRICE_RECOMMEND_CAMPAIGN_ARN


class GenderPersonalize(PersonalizeBase):
    AWS_REGION = settings.AWS_PERSONALIZE_GENDER_REGION
    PERSONALIZE_CAMPAIGN_ARN = settings.AWS_PERSONALIZE_GENDER_RECOMMEND_CAMPAIGN_ARN
    DEFAULT_SOLUTION_VERSION_ARN = settings.AWS_PERSONALIZE_GENDER_RECOMMEND_SOLUTION_VERSION_ARN
    TRACKING_ID = settings.AWS_PERSONALIZE_GENDER_TRACKING_ID
