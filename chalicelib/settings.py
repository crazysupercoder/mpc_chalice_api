import os
import json
from dotenv import load_dotenv
from pathlib import Path
from .constants import *

env_path = os.path.join(
    os.path.dirname(__file__),
    '.env')
load_dotenv(env_path)


class PERSONALIZE_RESOURCE_TYPE:
    campaign = 'campaign'
    solution = 'solution'


class Config:
    # ------------------------------------------------------------------------------------------------------------------
    #                                                   GENERAL
    # ------------------------------------------------------------------------------------------------------------------

    # General app info
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    APP_NAME_SUFFIX = os.environ.get('APP_NAME_SUFFIX')
    DEBUG = os.environ.get('DEBUG', False)
    STAGE = os.environ.get('STAGE', 'dev')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '917885688343')

    FRONTEND_BASE_URL = os.environ.get('FRONTEND_BASE_URL')

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   COGNITO
    # ------------------------------------------------------------------------------------------------------------------

    # AWS User Pool Configuration
    AWS_COGNITO_USER_POOL_NAME = os.environ.get('AWS_COGNITO_USER_POOL_NAME', 'mpc-dev-chalice-api-user-pool')
    AWS_COGNITO_USER_POOL_ID = os.environ.get('AWS_COGNITO_USER_POOL_ID', 'eu-west-1_UB4WIHfuT')
    AWS_COGNITO_USER_POOL_ARN = os.environ.get(
        'AWS_COGNITO_USER_POOL_ARN',
        'arn:aws:cognito-idp:eu-west-1:917885688343:userpool/%s' % AWS_COGNITO_USER_POOL_ID
    )

    # ------------------------------------------------------------------------------------------------------------------
    #                                               PERSONALIZATION
    # ------------------------------------------------------------------------------------------------------------------

    # AWS Peronalize Configuration
    AWS_PERSONALIZE_DEFAULT_REGION = os.environ.get('AWS_PERSONALIZE_DEFAULT_REGION', 'eu-west-1')
    
    def build_personalize_resource_name(
                resource_id: str,
                account_id: str = AWS_ACCOUNT_ID,
                region: str = AWS_PERSONALIZE_DEFAULT_REGION,
                resource_type: str = PERSONALIZE_RESOURCE_TYPE.campaign,
            ) -> str:
        AWS_PERSONALIZE_PATTERN = "arn:aws:personalize:{region}:%s" % account_id
        resource_arn_maps = {
            PERSONALIZE_RESOURCE_TYPE.campaign: "%s:%s/%s" % (
                AWS_PERSONALIZE_PATTERN, PERSONALIZE_RESOURCE_TYPE.campaign, '{id}'),
            PERSONALIZE_RESOURCE_TYPE.solution: "%s:%s/%s" % (
                AWS_PERSONALIZE_PATTERN, PERSONALIZE_RESOURCE_TYPE.solution, '{id}')
        }

        if resource_type not in resource_arn_maps:
            raise Exception("Unknown resource type - %s" % resource_type)
        return resource_arn_maps[resource_type].format(
            region=region, id=resource_id)
    

    ## SIMPLE SKU
    AWS_PERSONALIZE_SIMPLE_SKU_REGION = os.environ.get('AWS_PERSONALIZE_SIMPLE_SKU_REGION', 'us-east-1')
    AWS_PERSONALIZE_SIMPLE_SKU_RECOMMEND_CAMPAIGN_ARN = os.environ.get(
        'AWS_PERSONALIZE_SIMPLE_SKU_RECOMMEND_CAMPAIGN_ARN',
        'WE DO NOT USE THIS AT THE MOMENT'
    )

    ## CONFIG SKU
    AWS_PERSONALIZE_CONFIG_SKU_REGION = os.environ.get('AWS_PERSONALIZE_CONFIG_SKU_REGION', 'us-east-1')
    AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_CAMPAIGN_ARN',
            'mpc-personalize-config-sku-hrnn-with-meta-campaign'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_SOLUTION_VERSION_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_RECOMMEND_SOLUTION_VERSION_ARN',
            'mpc-personalize-config-sku-hrnn-with-meta-solution/4b268b3a'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_CAMPAIGN_ARN',
            'mpc-personalize-config-sku-similar-items-campaign'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_SOLUTION_VERSION_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_SIMILAR_ITEMS_SOLUTION_VERSION_ARN',
            'mpc-personalize-config-sku-similar-items-solution/6ac8121e'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_CONFIG_SKU_RANKING_CAMPAIGN_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_RANKING_CAMPAIGN_ARN',
            'mpc-personalize-config-sku-personalized-ranking-campaign'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_CONFIG_SKU_RANKING_SOLUTION_VERSION_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_CONFIG_SKU_RANKING_SOLUTION_VERSION_ARN',
            'mpc-personalize-config-sku-personalized-ranking-solution/568329b2'
        ),
        region=AWS_PERSONALIZE_CONFIG_SKU_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_CONFIG_SKU_TRACKING_ID = os.environ.get(
        'AWS_PERSONALIZE_CONFIG_SKU_TRACKING_ID',
        '06cc3041-438e-4e18-a4d8-fc9af087d728'
    )

    ## PRODUCT SIZE
    AWS_PERSONALIZE_SIZE_REGION = os.environ.get('AWS_PERSONALIZE_SIZE_REGION', 'eu-west-1')
    AWS_PERSONALIZE_SIZE_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_SIZE_RECOMMEND_CAMPAIGN_ARN',
            'mpc-personalize-product-size-hrnn-campaign'
        ),
        region=AWS_PERSONALIZE_SIZE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_SIZE_RECOMMEND_SOLUTION_VERSION_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_SIZE_RECOMMEND_SOLUTION_VERSION_ARN',
            'mpc-personalize-product-size-hrnn-with-meta/77caec95'
        ),
        region=AWS_PERSONALIZE_SIZE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_SIZE_RANKING_CAMPAIGN_ARN = build_personalize_resource_name(
            os.environ.get(
            'AWS_PERSONALIZE_SIZE_RANKING_CAMPAIGN_ARN',
            'mpc-personalize-product-size-personalized-ranking-campaign'
        ),
        region=AWS_PERSONALIZE_SIZE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_SIZE_TRACKING_ID = os.environ.get(
        'AWS_PERSONALIZE_SIZE_TRACKING_ID',
        '3adb63f0-4208-4186-a5d2-c90b41c40496'
    )

    ## PRODUCT BRAND
    AWS_PERSONALIZE_BRAND_REGION = os.environ.get('AWS_PERSONALIZE_BRAND_REGION', 'eu-west-1')
    AWS_PERSONALIZE_BRAND_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_BRAND_RECOMMEND_CAMPAIGN_ARN',
            'mpc-personalize-product-brand-hrnn-campaign'
        ),
        region=AWS_PERSONALIZE_BRAND_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_BRAND_RECOMMEND_SOLUTION_VERSION_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_BRAND_RECOMMEND_SOLUTION_VERSION_ARN',
            'mpc-personalize-product-brand-hrnn-with-meta/59b5757b'
        ),
        region=AWS_PERSONALIZE_BRAND_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_BRAND_TRACKING_ID = os.environ.get(
        'AWS_PERSONALIZE_BRAND_TRACKING_ID',
        'd5269b28-d223-42f5-b739-9853b9d304ca'
    )

    ## PRODUCT SIZE
    AWS_PERSONALIZE_PRODUCT_TYPE_REGION = os.environ.get(
        'AWS_PERSONALIZE_PRODUCT_TYPE_REGION', 'eu-west-1')
    AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_CAMPAIGN_ARN',
            'mpc-product-type-hrnn-cmp'
        ),
        region=AWS_PERSONALIZE_PRODUCT_TYPE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_SOLUTION_VERSION_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_PRODUCT_TYPE_RECOMMEND_SOLUTION_VERSION_ARN',
            'mpc-product-type-hrnn/b8d7e6e0'
        ),
        region=AWS_PERSONALIZE_PRODUCT_TYPE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_PRODUCT_TYPE_SIMILAR_ITEMS_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_PRODUCT_TYPE_SIMILAR_ITEMS_CAMPAIGN_ARN',
            'mpc-personalize-product-type-similar-items-campaign'
        ),
        region=AWS_PERSONALIZE_PRODUCT_TYPE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_PRODUCT_TYPE_RANKING_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_PRODUCT_TYPE_RANKING_CAMPAIGN_ARN',
            'mpc-personalize-product-type-ranking-campaign'
        ),
        region=AWS_PERSONALIZE_PRODUCT_TYPE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_PRODUCT_TYPE_TRACKING_ID = os.environ.get(
        'AWS_PERSONALIZE_PRODUCT_TYPE_TRACKING_ID',
        'bd1f8bff-c823-4cd3-bde7-a85b764f7210'
    )

    ## PRODUCT PRICE
    AWS_PERSONALIZE_PRICE_REGION = os.environ.get(
        'AWS_PERSONALIZE_PRICE_REGION', 'us-east-1')
    AWS_PERSONALIZE_PRICE_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_PRICE_RECOMMEND_CAMPAIGN_ARN',
            'mpc-personalize-price-campaign'
        ),
        region=AWS_PERSONALIZE_PRICE_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )

    ## GENDER
    AWS_PERSONALIZE_GENDER_REGION = os.environ.get(
        'AWS_PERSONALIZE_GENDER_REGION', 'us-east-1')
    AWS_PERSONALIZE_GENDER_RECOMMEND_CAMPAIGN_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_GENDER_RECOMMEND_CAMPAIGN_ARN',
            'mpc-user-gender-hrnn-cmp'
        ),
        region=AWS_PERSONALIZE_GENDER_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.campaign
    )
    AWS_PERSONALIZE_GENDER_RECOMMEND_SOLUTION_VERSION_ARN = build_personalize_resource_name(
        os.environ.get(
            'AWS_PERSONALIZE_GENDER_RECOMMEND_SOLUTION_VERSION_ARN',
            'mpc-personalize-gender-hrnn-solution/cf120bb7'
        ),
        region=AWS_PERSONALIZE_GENDER_REGION,
        resource_type=PERSONALIZE_RESOURCE_TYPE.solution
    )
    AWS_PERSONALIZE_GENDER_TRACKING_ID = os.environ.get(
        'AWS_PERSONALIZE_GENDER_TRACKING_ID',
        'd153e8c8-88c4-4723-b588-cc59f46f3b1f'
    )

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   DYNAMO DB
    # ------------------------------------------------------------------------------------------------------------------

    # AWS DYNAMO TABLE CONFIG
    AWS_DYNAMODB_DEFAULT_REGION = os.environ.get('AWS_DYNAMODB_DEFAULT_REGION', 'eu-west-1')
    AWS_DYNAMODB_CMS_TABLE_NAME = os.environ.get('AWS_DYNAMODB_CMS_TABLE_NAME', 'CMS')
    AWS_DYNAMODB_MAGENTO_CUSTOMER_TABLE_NAME = os.environ.get('AWS_DYNAMODB_MAGENTO_CUSTOMER_TABLE_NAME', 'Magento')
    AWS_DYNAMODB_BANNER_TABLE_NAME = os.environ.get('AWS_DYNAMODB_BANNER_TABLE_NAME', 'Banners')

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   ELASTIC
    # ------------------------------------------------------------------------------------------------------------------

    # AWS Elasticsarch Service configuration
    AWS_ELASTICSEARCH_PRODUCTS_REGION = os.environ.get('AWS_ELASTICSEARCH_PRODUCTS_REGION', 'eu-west-1')
    AWS_ELASTICSEARCH_SCHEMA = os.environ.get('AWS_ELASTICSEARCH_SCHEMA', 'https')
    AWS_ELASTICSEARCH_HOST = os.environ.get(
        'AWS_ELASTICSEARCH_HOST',
        'search-mpc-domain-qhdgnvecvaqb77evx7i64zbldm.eu-west-1.es.amazonaws.com')
    AWS_ELASTICSEARCH_PORT = int(os.environ.get('AWS_ELASTICSEARCH_PORT', 443))
    AWS_ELASTICSEARCH_ENDPOINT = '{}://{}:{}'.format(
        AWS_ELASTICSEARCH_SCHEMA,
        AWS_ELASTICSEARCH_HOST,
        AWS_ELASTICSEARCH_PORT
    )
    AWS_ELASTICSEARCH_SCROLL_LIFETIME = os.environ.get('AWS_ELASTICSEARCH_SCROLL_LIFETIME', '5m')

    # products
    AWS_ELASTICSEARCH_PRODUCTS = os.environ.get('AWS_ELASTICSEARCH_PRODUCTS', 'products')
    AWS_ELASTICSEARCH_PRODUCTS_TRACKING_COUNTERS = os.environ.get(
        'AWS_ELASTICSEARCH_PRODUCTS_TRACKING_COUNTERS',
        'products_tracking_counters'
    )
    AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION = os.environ.get(
        'AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION',
        'products_tracking_user_action'
    )
    AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION_PRODUCT_SNAPSHOT = os.environ.get(
        'AWS_ELASTICSEARCH_PRODUCTS_TRACKING_USER_ACTION_PRODUCT_SNAPSHOT',
        'products_tracking_user_action_product_snapshot'
    )

    # Scored Products
    AWS_ELASTICSEARCH_SCORED_PRODUCTS = os.environ.get(
        'AWS_ELASTICSEARCH_SCORED_PRODUCTS', 'scored_products')

    # orders
    AWS_ELASTICSEARCH_PURCHASE_ORDERS = os.environ.get('AWS_ELASTICSEARCH_PURCHASE_ORDERS', 'purchase_orders')
    AWS_ELASTICSEARCH_PURCHASE_ORDERS_CUSTOMER_ORDERS_MAP = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_ORDERS_CUSTOMER_ORDERS_MAP',
        'purchase_orders_customer_orders_map'
    )

    # credit cards
    AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS',
        'purchase_customer_credit_cards'
    )
    AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS_CUSTOMER_MAP = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_CUSTOMER_CREDIT_CARDS_CUSTOMER_MAP',
        'purchase_customer_credit_cards_customer_map'
    )

    # returns
    AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS',
        'purchase_return_requests'
    )
    AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS_CUSTOMER_MAP = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_RETURN_REQUESTS_CUSTOMER_MAP',
        'purchase_return_requests_customer_map'
    )

    # cancels
    AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS',
        'purchase_cancel_requests'
    )
    AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS_ORDERS_MAP = os.environ.get(
        'AWS_ELASTICSEARCH_PURCHASE_CANCEL_REQUESTS_ORDERS_MAP',
        'purchase_cancel_requests_orders_map'
    )

    # personalization
    AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS = os.environ.get(
        'AWS_ELASTICSEARCH_PERSONALIZATION_ORDERS',
        'personalization_orders'
    )

    # delta cache
    AWS_ELASTICSEARCH_DELTA_CACHE = os.environ.get('AWS_ELASTICSEARCH_DELTA_CACHE', 'delta_cache')

    # customer tiers
    AWS_ELASTICSEARCH_CUSTOMER_TIERS_TIERS = os.environ.get(
        'AWS_ELASTICSEARCH_CUSTOMER_TIERS_TIERS',
        'customer_tiers_tiers'
    )
    AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS = os.environ.get(
        'AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_TIERS',
        'customer_tiers_customer_tiers'
    )
    AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT = os.environ.get(
        'AWS_ELASTICSEARCH_CUSTOMER_TIERS_CUSTOMER_INFO_SPENT_AMOUNT',
        'customer_tiers_customer_info_spent_amount'
    )

    # fbucks
    AWS_ELASTICSEARCH_FBUCKS_HANDLED_ORDERS = os.environ.get(
        'AWS_ELASTICSEARCH_FBUCKS_HANDLED_ORDERS',
        'fbucks_handled_orders'
    )
    AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT = os.environ.get(
        'AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT',
        'fbucks_customer_amount'
    )
    AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES = os.environ.get(
        'AWS_ELASTICSEARCH_FBUCKS_CUSTOMER_AMOUNT_CHANGES',
        'fbucks_customer_amount_changes'
    )

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   SQS QUEUE
    # ------------------------------------------------------------------------------------------------------------------

    PORTAL_AWS_ACCOUNT_ID = os.environ.get('PORTAL_AWS_ACCOUNT_ID', AWS_ACCOUNT_ID)
    SQS_REGION = os.environ.get('SQS_REGION', 'eu-west-1')
    def build_sqs_url(
        queue_name: str,
        account_id: str = AWS_ACCOUNT_ID,
        region: str = SQS_REGION
    ) -> str:
        if not os.environ.get('DEBUG', False) and not queue_name or not account_id or not region:
            raise Exception("Your configuration has a fatal error.")

        return 'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'.format(
            account_id=account_id, region=region, queue_name=queue_name)


    # Building SQS Queue URL
    SQS_MPC_PORTAL_COMMON = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_COMMON'))
    SQS_MPC_PORTAL_ORDER = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_ORDER'), account_id=PORTAL_AWS_ACCOUNT_ID)
    SQS_MPC_PORTAL_EMAIL_SUBSCRIPTION = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_EMAIL_SUBSCRIPTION'))
    SQS_MPC_PORTAL_CUSTOMER_INFO_REQUEST = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_CUSTOMER_INFO_REQUEST'))
    SQS_MPC_PORTAL_COMMUNICATION_PREFERENCES = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_COMMUNICATION_PREFERENCES'))
    SQS_MPC_MPC_COMMON_URL = build_sqs_url(os.environ.get('SQS_MPC_MPC_COMMON_NAME'))
    SQS_MPC_PORTAL_CUSTOMER_INFO_UPDATE = build_sqs_url(os.environ.get('SQS_MPC_PORTAL_CUSTOMER_INFO_UPDATE'))

    # { queues: [{ name: str, batch_size: int }, ...] }
    SQS_LISTENER_CONFIG = {
        'queues': [
            {'name': os.environ.get('SQS_PORTAL_MPC_COMMON'), 'batch_size': 1},
            {'name': os.environ.get('SQS_PORTAL_MPC_ORDER'), 'batch_size': 1},
            {'name': os.environ.get('SQS_MPC_MPC_COMMON_NAME'), 'batch_size': 1},
            {'name': os.environ.get('SQS_PORTAL_MPC_CUSTOMER_INFO_UPDATE'), 'batch_size': 1},
        ]
    }

    # { event_descriptor: { object_type: str, queue_url: str, ... } }
    SQS_SENDER_CONFIG = {
        # can be used for local
        # 'class': 'chalicelib.libs.core.sqs_sender._SqsSenderDummyPrint',
        # 'params': {},

        # @TODO : use mpc-portal-common instead of not critical mpc-portal queues
        # @TODO : create single listener of mpc-portal-common for not critical mpc-portal messages

        'class': 'chalicelib.libs.core.sqs_sender._SqsSenderSqs',
        'params': {
            'events': {
                'user_answer': {
                    'object_type': 'user_answer',
                    'queue_url': SQS_MPC_PORTAL_COMMON,
                },
                'communication_preferences': {
                    'object_type': 'communication_preferences',
                    'queue_url': SQS_MPC_PORTAL_COMMUNICATION_PREFERENCES,
                },
                'credit_cash_out_request': {
                    'object_type': 'credit_cash_out_request',
                    'queue_url': SQS_MPC_PORTAL_COMMON,
                },
                "customer_info_request": {
                    "object_type": "customer_info_request",
                    "queue_url": SQS_MPC_PORTAL_CUSTOMER_INFO_REQUEST,
                },
                'contactus_request': {
                    'object_type': 'contactus_request',
                    'queue_url': SQS_MPC_PORTAL_COMMON,
                },
                'order_change': {
                    'object_type': 'mpc_order',
                    'queue_url': SQS_MPC_PORTAL_ORDER,
                },
                'eft_proof_uploaded': {
                    'object_type': 'eft_proof_uploaded',
                    'queue_url': SQS_MPC_PORTAL_ORDER,
                },
                'return_request_change': {
                    'object_type': 'return_request_change',
                    'queue_url': SQS_MPC_PORTAL_ORDER,
                },
                'fixel_paid_order_cancellation_request': {
                    'object_type': 'fixel_paid_order_cancellation_request',
                    'queue_url': SQS_MPC_PORTAL_ORDER,
                },
                'subscription_subscribed': {
                    'object_type': 'subscription_subscribed',
                    'queue_url': SQS_MPC_PORTAL_EMAIL_SUBSCRIPTION,
                },
                'subscription_unsubscribed': {
                    'object_type': 'subscription_unsubscribed',
                    'queue_url': SQS_MPC_PORTAL_EMAIL_SUBSCRIPTION,
                },
                DELTA_CACHE_MESSAGE_TYPES.CACHE_UPDATE: {
                    'object_type': DELTA_CACHE_MESSAGE_TYPES.CACHE_UPDATE,
                    'queue_url': SQS_MPC_MPC_COMMON_URL,
                },
                SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER: {
                    'object_type': SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER,
                    'queue_url': SQS_MPC_MPC_COMMON_URL,
                },
                'customer_info_update': {
                    'object_type': 'customer_info_update',
                    'queue_url': SQS_MPC_PORTAL_CUSTOMER_INFO_UPDATE,
                },
            }
        }
    }

    # ------------------------------------------------------------------------------------------------------------------
    #                                                   MAILER
    # ------------------------------------------------------------------------------------------------------------------

    MAILER_CONFIG = json.loads(os.environ.get('MAILER_CONFIG', json.dumps({
        # can be used for local
        # 'class': 'chalicelib.libs.core.mailer._MailerDummyPrint',
        # 'params': {},

        # live
        'class': 'chalicelib.libs.core.mailer._MailerSmtp',
        'params': {
            'from_email': 'portal@runwaysale.co.za',
            'host': 'smtp.mandrillapp.com',
            'port': 587,
            'username': 'info@runwaysale.co.za',
            'password': 'vAkn_tSiZMbqU-KFAZwOlA',
        }
    })))

    # ------------------------------------------------------------------------------------------------------------------
    #                                               FILE STORAGE
    # ------------------------------------------------------------------------------------------------------------------

    FILE_STORAGE_CONFIG = json.loads(os.environ.get('FILE_STORAGE_CONFIG', json.dumps({
        # This is config example for local environment. Change it in your own run script.
        # Implementations for other environments are defined in config.json.
        #
        # export FILE_STORAGE_CONFIG='{
        #   "class": "chalicelib.libs.core.file_storage._FileLocalStorage",
        #   "params": {"root_path": "/var/www/html/mpc_api_storage", "root_url": "http://localhost/mpc_api_storage/"}
        # }'
    })))

    # ------------------------------------------------------------------------------------------------------------------
    #                                                  OTHER
    # ------------------------------------------------------------------------------------------------------------------

    # Delivery API
    DTD_API_DEFAULT_DTD_URL = os.environ.get('DTD_API_DEFAULT_DTD_URL', 'https://cdt.runway.co.za/sku/DEFAULT')
    DTD_API_DEFAULT_DTD_MIN = os.environ.get('DTD_API_DEFAULT_DTD_MIN', 10)  # if default api is unavailable,
    DTD_API_DEFAULT_DTD_MAX = os.environ.get('DTD_API_DEFAULT_DTD_MAX', 25)  # we should use hardcoded values
    DTD_API_SKU_BASE_URL = os.environ.get('DTD_API_SKU_BASE_URL', 'https://cdt.runway.co.za/sku/')

    # Product filtering meta data
    NEW_PRODUCT_THRESHOLD = int(os.environ.get('NEW_PRODUCT_THRESHOLD', 365))  # Should be 7 days in production
    LAST_CHANCE_STOCK_THRESHOLD = os.environ.get('LAST_CHANCE_STOCK_THRESHOLD', 10)  # Stock Number
    LAST_CHANCE_END_DATE_THRESHOLD = os.environ.get('LAST_CHANCE_END_DATE_THRESHOLD', 30)
    PRODUCT_VISIT_LOG_MAX = os.environ.get('PRODUCT_VISIT_LOG_MAX', 10)
    PRODUCT_VISIT_LOG_THRESHOLD = os.environ.get('PRODUCT_VISIT_LOG_THRESHOLD', 7)

    # READ API
    READ_API_HEADER_NAME = os.environ.get('READ_API_HEADER_NAME', 'Identification')
    READ_API_HEADER_VALUE = os.environ.get('READ_API_HEADER_VALUE', 'RunwaySale::ReadAPI')

    # PEACH PAYMENT
    # https://peachpayments.docs.oppwa.com/
    # Attention! Default values are for tests here (see doc/examples).
    PEACH_PAYMENT_BASE_URL = os.environ.get('PEACH_PAYMENT_BASE_URL', 'https://test.oppwa.com/v1/')
    PEACH_PAYMENT_ENTITY_ID = os.environ.get('PEACH_PAYMENT_ENTITY_ID', '8a8294174e735d0c014e78cf26461790')
    PEACH_PAYMENT_ACCESS_TOKEN = os.environ.get(
        'PEACH_PAYMENT_ACCESS_TOKEN',
        'OGE4Mjk0MTc0ZTczNWQwYzAxNGU3OGNmMjY2YjE3OTR8cXl5ZkhDTjgzZQ=='
    )
    PEACH_PAYMENT_WEBHOOKS_DECRYPTION_KEY = os.environ.get('PEACH_PAYMENT_WEBHOOKS_DECRYPTION_KEY', "need_real_value")

    # ------------------------------------------------------------------------------------------------------------------

    # CRITICAL! FOR THE DATA LAKE
    DATALAKE_AWS_ACCOUNT_ACCESS_KEY_ID = os.environ.get(
        'DATALAKE_AWS_ACCOUNT_ACCESS_KEY_ID')
    DATALAKE_AWS_ACCOUNT_SECRET_KEY_ID = os.environ.get(
        'DATALAKE_AWS_ACCOUNT_SECRET_KEY_ID')
    DATALAKE_USERTRACK_DELIVERY_STREAM_NAME = os.environ.get(
        'DATALAKE_USERTRACK_DELIVERY_STREAM_NAME')


settings = Config()

