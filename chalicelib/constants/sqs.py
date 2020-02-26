__all__ = [
    'DELTA_CACHE_MESSAGE_TYPES', 'SCORED_PRODUCT_MESSAGE_TYPE']


class DELTA_CACHE_MESSAGE_TYPES:
    CACHE_UPDATE = 'delta_cache_update'
    SECRET_KEY = 'mpc_secret_key'


class SCORED_PRODUCT_MESSAGE_TYPE:
    CALCULATE_PRODUCT_SCORE = 'calculate_product_score'
    CALCULATE_FOR_A_CUSTOMER = 'calculate_for_a_customer'
