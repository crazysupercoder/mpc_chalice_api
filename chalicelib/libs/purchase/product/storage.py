from typing import Optional
from chalicelib.extensions import *
from chalicelib.libs.models.mpc.Product import Product as MpcProducts
from chalicelib.libs.purchase.core import SimpleSku, ProductInterface, ProductStorageInterface
from .product import ProductInterfaceImplementation


class ProductStorageImplementation(ProductStorageInterface):
    def __init__(self):
        self.__mpcProducts = MpcProducts()

    def load(self, simple_sku: SimpleSku) -> Optional[ProductInterface]:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.load, 'simple_sku', simple_sku)

        data = self.__mpcProducts.getRawDataBySimpleSku(simple_sku.value)
        if not data:
            return None

        product = ProductInterfaceImplementation(data, simple_sku.value)
        return product

    def update(self, product: ProductInterface) -> None:
        if not isinstance(product, ProductInterface):
            raise ArgumentTypeException(self.update, 'product', product)

        raw_product_data = self.__mpcProducts.getRawDataBySimpleSku(product.simple_sku.value, False)
        if not raw_product_data:
            raise ValueError('Product "{}" does not exist, so cannot be updated!'.format(product.simple_sku.value))

        for size_data in raw_product_data.get('sizes', []):
            if size_data.get('rs_simple_sku') == product.simple_sku.value:
                size_data['qty'] = product.qty_available.value
                break

        self.__mpcProducts.update(raw_product_data.get('rs_sku'), raw_product_data)

