from ...settings import settings
from .base import *
from chalicelib.libs.models.ml.products import Product as MlProducts
from chalicelib.libs.models.mpc.Product import Product as MpcProducts
from chalicelib.libs.models.ml.scored_products import ScoredProduct


class ProductSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__product_model = MlProducts()

    def handle(self, sqs_message: SqsMessage) -> None:
        message_type = sqs_message.message_type

        if message_type == 'mpc_assets_product_config':
            self.__handle_save(sqs_message)
        else:
            raise ValueError('SQS Message type "' + message_type + '" is unknown for ' + self.__class__.__name__)

    def __handle_save(self, sqs_message: SqsMessage) -> None:
        products = sqs_message.message_data

        self.__product_model.bulk_insert(
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            products
        )


# ----------------------------------------------------------------------------------------------------------------------


class EventProductSqsHandler(SqsHandlerInterface):
    def handle(self, sqs_message: SqsMessage) -> None:
        from chalicelib.libs.models.mpc.ProductMapping import mapping
        ml_products = MlProducts()
        items = sqs_message.message_data
        ml_products.create_index(
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            mapping, recreate=True)
        ml_products.bulk_insert(
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            settings.AWS_ELASTICSEARCH_PRODUCTS,
            items, random_date=True)


# ----------------------------------------------------------------------------------------------------------------------


class StockSqsHandler(SqsHandlerInterface):
    def handle(self, sqs_message: SqsMessage) -> None:
        if 'products' in sqs_message.message_data:#bulk update
            items = sqs_message.message_data['products']
        else:#single update
            if isinstance(sqs_message.message_data, list):
                items = sqs_message.message_data
            else:
                items = list(sqs_message.message_data)

        MpcProducts().updateStock(items)
        ScoredProduct().updateStock(items)


# ----------------------------------------------------------------------------------------------------------------------


class SingleProductSqsHandler(SqsHandlerInterface):
    def handle(self, sqs_message: SqsMessage) -> None:
        MpcProducts().update(sqs_message.message_data['rs_sku'], sqs_message.message_data)
        ScoredProduct().update(
            sqs_message.message_data['rs_sku'],
            sqs_message.message_data)


# ----------------------------------------------------------------------------------------------------------------------

