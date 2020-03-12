from chalicelib.extensions import *
from chalicelib.constants import *
from chalicelib.settings import settings
from chalicelib.libs.core.logger import Logger
from chalicelib.libs.core.chalice import MPCApi, Rate
from chalicelib.endpoints.brands.base import brands_blueprint
from chalicelib.endpoints.banners.base import banners_blueprint
from chalicelib.endpoints.accounts.base import accounts_blueprint
from chalicelib.endpoints.products.base import products_blueprint
from chalicelib.endpoints.ml.base import ml_blueprint
from chalicelib.endpoints.messages.base import blueprint as messages_blueprint
from chalicelib.endpoints.magento.base import magento_blueprint
from chalicelib.endpoints.purchase.base import blueprint as purchase_blueprint
from chalicelib.endpoints.read_api.base import blueprint as read_api_blueprint
from chalicelib.endpoints.tracking.base import blueprint as tracking_blueprint
from chalicelib.endpoints.invite_friends.base import invite_friends_blueprint
from chalicelib.endpoints.subscription.base import blueprint as subscription_blueprint
from chalicelib.endpoints.static_page.base import blueprint as static_page_blueprint
from chalicelib.endpoints.search.base import blueprint as search_blueprint
from chalicelib.endpoints.tech.base import blueprint as tech_blueprint
from chalicelib.endpoints.wish.base import wish_blueprint
from chalicelib.endpoints.seen.base import seen_blueprint
from chalicelib.endpoints.credit.base import credit_blueprint
from chalicelib.endpoints.contactus.base import contactus_blueprint
from chalicelib.libs.models.mpc.user import User
from chalicelib.libs.models.mpc.Cms.user_states import CustomerStateModel
# import processes
from chalicelib.utils.sqs_handlers.base import *

app = MPCApi(app_name='mpc-chalice-api-%s' % settings.APP_NAME_SUFFIX)
app.experimental_feature_flags.update(['BLUEPRINTS'])
app.register_blueprint(brands_blueprint, name_prefix='brands', url_prefix='/brands')
app.register_blueprint(banners_blueprint, name_prefix='banners', url_prefix='/banners')
app.register_blueprint(accounts_blueprint, name_prefix='accounts', url_prefix='/accounts')
app.register_blueprint(products_blueprint, name_prefix='products', url_prefix='/products')
app.register_blueprint(ml_blueprint, name_prefix='ml', url_prefix='/ml')
app.register_blueprint(messages_blueprint, name_prefix='messages', url_prefix='/messages')
app.register_blueprint(magento_blueprint, name_prefix='magento', url_prefix='/auth')
app.register_blueprint(purchase_blueprint, name_prefix='purchase', url_prefix='/purchase')
app.register_blueprint(read_api_blueprint, name_prefix='read-api', url_prefix='/read-api')
app.register_blueprint(tracking_blueprint, name_prefix='tracking', url_prefix='/tracking')
app.register_blueprint(invite_friends_blueprint, name_prefix='invite-friends', url_prefix='/invite-friends')
app.register_blueprint(subscription_blueprint, name_prefix='subscription', url_prefix='/subscription')
app.register_blueprint(static_page_blueprint, name_prefix='static_page', url_prefix='/static_page')
app.register_blueprint(search_blueprint, name_prefix='search', url_prefix='/search')
app.register_blueprint(tech_blueprint, name_prefix='tech', url_prefix='/tech')
app.register_blueprint(wish_blueprint, name_prefix='wish', url_prefix='/wish')
app.register_blueprint(seen_blueprint, name_prefix='seen', url_prefix='/seen')
app.register_blueprint(credit_blueprint, name_prefix='credit', url_prefix='/credit')
app.register_blueprint(contactus_blueprint, name_prefix='contactus', url_prefix='/contactus')
app.debug = settings.DEBUG


# ----------------------------------------------------------------------------------------------------------------------
#                                                   SQS LISTENERS
# ----------------------------------------------------------------------------------------------------------------------


def __handle_sqs_message(event):
    import json

    logger = Logger()

    for record in event:
        data = record.to_dict()
        object_type = data['messageAttributes']['object_type']['stringValue']

        sqs_message_data = record.body if type(record.body) is dict else json.loads(record.body)
        sqs_message = SqsMessage(data['messageId'], object_type, sqs_message_data)
        logger.log_simple('SQS Event Handling - Handle message "{}" #{} - Start'.format(
            sqs_message.message_type,
            sqs_message.id
        ))

        handlers_map = {
            'chalicelib.utils.sqs_handlers.product.ProductSqsHandler': ('mpc_assets_product_config',),
            'chalicelib.utils.sqs_handlers.static_page.StaticPageSqsHandler': (
                'static_page_publish',
                'static_page_unpublish'
            ),
            'chalicelib.utils.sqs_handlers.sticker.StickerSqsHandler': ('product_sticker', 'product_sticker_delete'),
            'chalicelib.utils.sqs_handlers.user_question.UserQuestionSqsHandler': ('user_question',),
            'chalicelib.utils.sqs_handlers.banner.BannerSqsHandler': ('mpc_banner', 'mpc_banner_delete'),
            'chalicelib.utils.sqs_handlers.product_type.ProductTypeSqsHandler': (
                'mpc_assets_product_type',
                'mpc_assets_product_type_delete',
            ),
            'chalicelib.utils.sqs_handlers.category.CategorySqsHandler': ('mpc_assets_category_delete',),
            'chalicelib.utils.sqs_handlers.brand.BrandSqsHandler': ('mpc_assets_brands', 'mpc_assets_brands_delete'),
            'chalicelib.utils.sqs_handlers.personalization.OrderHandler': ('personalization_order',),

            # @todo : merge into products queue handler ???
            'chalicelib.utils.sqs_handlers.product.EventProductSqsHandler': ('event_products',),
            'chalicelib.utils.sqs_handlers.product.StockSqsHandler': ('stock_update',),
            'chalicelib.utils.sqs_handlers.product.SingleProductSqsHandler': ('single_product', 'image_update'),

            'chalicelib.utils.sqs_handlers.scored_product.ScoredProductSqsHandler': (
                SCORED_PRODUCT_MESSAGE_TYPE.CALCULATE_FOR_A_CUSTOMER, SCORED_PRODUCT_MESSAGE_TYPE.SECRET_KEY),

            'chalicelib.libs.purchase.order.sqs.OrderChangeSqsHandler': ('order_change',),
            'chalicelib.libs.purchase.order.sqs.OrderRefundSqsHandler': ('fixel_order_refund',),
            'chalicelib.libs.purchase.order.sqs.OrderPaymentOhHoldHandler': ('fixel_order_on_hold_by_portal',),
            'chalicelib.libs.purchase.payment_methods.regular_eft.sqs.RegularEftPaymentSqsHandler': ('regular_eft_proof_check_result',),
            'chalicelib.libs.purchase.cancellations.sqs.CancelRequestPaidOrderAnswerSqsHandler': ('fixel_paid_order_cancellation_request_answer',),
            'chalicelib.libs.purchase.cancellations.sqs.CancelledOrderOnPortalSideSqsHandle': ('fixel_order_cancellation_by_portal',),
            'chalicelib.libs.purchase.returns.sqs.ReturnRequestChangeSqsHandler': ('return_request_answer',),
            'chalicelib.libs.purchase.settings.PurchaseSettingsSqsHandler': ('dynamic_delivery_fees', 'parameters'),
            'chalicelib.libs.purchase.customer.sqs.CustomerTiersTiersSqsHandler': ('customer_tiers_set',),
            'chalicelib.libs.purchase.customer.sqs.CustomerTiersCustomersSqsHandler': ('customer_tiers_customers',),
            'chalicelib.libs.purchase.customer.sqs.FbucksChargeSqsHandler': ('fbucks_charge',),
            'chalicelib.libs.purchase.customer.sqs.CrutchCustomerSpentAmountSqsHandler': ('customer_info_spent_amount',),
            'chalicelib.libs.purchase.customer.sqs.CrutchCustomerInfoRequestAnswerSqsHandler': ('customer_info_request_answer',),

            'chalicelib.libs.credit.sqs.UserCreditBalanceSqsHandler': ('user_credit_balance',),
            'chalicelib.libs.informations.sqs.InformationsSqsHandler': ('customer_info',),
        }

        try:
            for class_name in handlers_map:
                if object_type in handlers_map[class_name]:
                    sqs_handler: SqsHandlerInterface = create_object(class_name)
                    sqs_handler.handle(sqs_message)
                    logger.log_simple('SQS Event Handling - Handle message "{}" #{} - Done!'.format(
                        sqs_message.message_type,
                        sqs_message.id
                    ))
                    return
            else:
                raise ValueError('SQS Handler was not found!')

        except BaseException as e:
            app.log.exception('Error SQS "{}" {} - {}'.format(object_type, data, str(e)))
            logger.log_simple('SQS Event Handling - Handle message "{}" #{} - Error: {}!'.format(
                sqs_message.message_type,
                sqs_message.id,
                str(e)
            ))


queues = settings.SQS_LISTENER_CONFIG.get('queues')
if not isinstance(queues, (tuple, list, set)) or sum([
    not isinstance(queue, dict)

    or 'name' not in queue.keys()
    or not isinstance(queue.get('name'), str)
    or not queue.get('name').strip()

    or 'batch_size' not in queue.keys()
    or not isinstance(queue.get('batch_size'), int)
    or queue.get('batch_size') <= 0

    for queue in queues
]) > 0:
    raise Exception('Incorrect SQS Receiver config! {} expects, but {} is given!'.format(
        '[{ name: str, batch_size: int }, ...]',
        queues
    ))


# preventing sqs handler lambda function bounding
# When you failed to deploy on your local,
# please comment the `if` statement, and deploy and rollback to deploy again.
if settings.STAGE in settings.STAGES_TO_BIND_LAMBDA_WITH_AWS_RESOURCES:
    shorten_stage_str = settings.STAGE
    if shorten_stage_str == 'production':
        shorten_stage_str = 'prod'
    @app.schedule(
        Rate(settings.SCORE_CALCULATE_INTERVAL, Rate.MINUTES),
        name='product-score-%s-schduler' % shorten_stage_str)
    def scheduler(event):
        emails = CustomerStateModel.get_customers_to_recalculate_scores()
        User.send_calculate_product_score_for_customers(emails)

    @app.lambda_function(name='cognito-hook-%s' % settings.STAGE)
    def cognito_hook(event, context):
        # TODO: Some logics that should be done when a customer was created here.
        trigger_source = event['triggerSource']
        if trigger_source == 'PostConfirmation_ConfirmSignUp':
            email = event['request']['userAttributes']['email']
            User.send_calculate_product_score_for_customers(emails=[email])
            return {'status': True}
        return {'status': False}

    for queue in queues:
        name = 'sqs_' + str(queue.get('name')).replace('.', '-')    # "." is not supported
        @app.on_sqs_message(queue=queue.get('name'), batch_size=queue.get('batch_size'), name=name)
        def register_listener(event):
            __handle_sqs_message(event)
else:
    print("Skipping lambda functions additional resources such as\n"\
        "- SQS queues\n"
        "- Cognito Hook\n"\
        "- Scheduler function")


# ----------------------------------------------------------------------------------------------------------------------

