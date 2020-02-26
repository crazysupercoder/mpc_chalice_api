import os.path
import json
import pandas as pd
import numpy as np
from decimal import Decimal
from ..libs.models.mpc.brands import Brand
from ..libs.models.mpc.product_types import ProductType
from .config import BASE_DIR


# constants
BRAND_MOCK_FILE_NAME = 'brands.csv'
PRODUCT_TYPE_MOCK_FILE_NAME = 'product_types.csv'
INDEX_NAME = 's3-to-es'
TYPE_NAME = 'brands'


def import_brands():
    brand_model = Brand()
    mock_file_path = os.path.join(BASE_DIR, 'mocks', BRAND_MOCK_FILE_NAME)
    if not os.path.exists(mock_file_path):
        raise FileNotFoundError()
    df = pd.read_csv(mock_file_path)
    brands = df.to_dict('records')
    filtered = []
    for item in brands:
        row ={
            'pk': 'BRAND',
            'sk': str(item['brand_id']),
            'brand': str(item['brand_name']),
            'brand_name': str(item['brand_name']).lower()
        }
        if not pd.isnull(item['code']):
            row['brand_code'] = str(item['code'])
        if not pd.isnull(item['logo_url']):
            row['brand_image_url'] = str(item['logo_url'])
        filtered.append(row)
    
    response = brand_model.insert_data(filtered, bulk_flag=True)
    print("%d brands successfully added to Dynamodb" % len(brands))


def import_product_types():
    product_type_model = ProductType()
    mock_file_path = os.path.join(BASE_DIR, 'mocks', PRODUCT_TYPE_MOCK_FILE_NAME)
    if not os.path.exists(mock_file_path):
        raise FileNotFoundError()
    df = pd.read_csv(mock_file_path)
    product_types = df.to_dict('records')
    filtered = []
    for item in product_types:
        row ={
            'pk': 'PRODUCT_TYPE',
            'sk': str(item['product_type_id']),
            'product_type_id': int(item['product_type_id']),
            'product_type_name': str(item['product_type_name']),
            'parent_id': int(item['product_type_parent_id']) if not pd.isnull(item.get('product_type_parent_id')) else 0,
            'product_type_code': str(item['product_type_code']),
            'product_gender_id': int(item['gender_id']) if not pd.isnull(item['gender_id']) else 0,
            'product_gender': item['product_gender_name'] if not pd.isnull(item['product_gender_name']) else None,
            'image': item['image'] if not pd.isnull(item.get('image')) else 'http://lorempixel.com/100/100/people/'
        }
        filtered.append(row)
    
    response = product_type_model.insert_data(filtered, bulk_flag=True)
    print("%d documents successfully added to Dynamodb" % len(filtered))
