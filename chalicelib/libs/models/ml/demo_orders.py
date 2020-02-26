from typing import List
from .order_temp_mock import mock_orders


class OrderAggregation:
    genders: List[str] = []
    colors: List[str] = []
    sizes: List[str] = []
    product_types: List[str] = []
    brands: List[str] = []
    product_count: int = 500

    def __init__(self, product_count: int = 500):
        self.product_count = product_count

    def append_gender(self, gender: str):
        if gender.lower() not in self.genders:
            self.genders.append(gender.lower())

    def append_color(self, color: str):
        if type(color) is str:
            color = color.lower()
        if color not in self.colors:
            self.colors.append(color)

    def append_size(self, size: str):
        if type(size) == str:
            size = size.lower()
        if size not in self.sizes:
            self.sizes.append(size)

    def append_product_type(self, product_type: str):
        if product_type.lower() not in self.product_types:
            self.product_types.append(product_type.lower())

    def append_brand(self, brand: str):
        if brand.lower() not in self.brands:
            self.brands.append(brand.lower())

    @property
    def gender_score(self):
        return self.product_count / max(1, len(self.genders))

    @property
    def color_score(self):
        return self.product_count / max(1, len(self.colors))

    @property
    def size_score(self):
        return self.product_count / max(1, len(self.sizes))

    @property
    def brand_score(self):
        return self.product_count / max(1, len(self.brands))

    @property
    def product_type_score(self):
        return self.product_count / max(1, len(self.product_types))

    @property
    def score_factors(self) -> dict:
        return {
            "gender": {
                "values": self.genders,
                "score": self.gender_score},
            "rs_color": {
                "values": self.colors,
                "score": self.color_score},
            "product_size_attribute": {
                "values": self.product_types,
                "score": self.product_type_score},
            "sizes.size.size": {
                "values": self.sizes,
                "score": self.size_score},
            "manufacturer": {
                "values": self.brands,
                "score": self.brand_score},
        }


class Order:
    email: str
    rs_sku: str
    rs_simple_sku: str
    product_name: str
    manufacturer: str
    gender: str
    product_size_attribute: str
    rs_colour: str
    size: str
    ordered_at: str

    def __init__(
            self,
            email: str = None,
            rs_sku: str = None,
            rs_simple_sku: str = None,
            product_name: str = None,
            manufacturer: str = None,
            gender: str = None,
            product_size_attribute: str = None,
            rs_colour: str = None,
            size: str = None,
            ordered_at: str = None,
            **kwargs):
        self.email = email
        self.rs_sku = rs_sku
        self.rs_simple_sku = rs_simple_sku
        self.product_name = product_name
        self.manufacturer = manufacturer
        self.gender = gender
        self.product_size_attribute = product_size_attribute
        self.rs_colour = rs_colour
        self.size = size
        self.ordered_at = ordered_at

    def to_dict(self):
        return {
            'email': self.email,
            'rs_sku': self.rs_sku,
            'rs_simple_sku': self.rs_simple_sku,
            'product_name': self.product_name,
            'manufacturer': self.manufacturer,
            'gender': self.gender,
            'product_size_attribute': self.product_size_attribute,
            'rs_colour': self.rs_colour,
            'size': self.size,
            'ordered_at': self.ordered_at
        }

    @classmethod
    def load_mock_orders(cls):
        return [cls(**item) for item in mock_orders]

    @classmethod
    def get_orders_for_customer(cls, email: str) -> OrderAggregation:
        agg = OrderAggregation()
        orders = [cls(**item) for item in mock_orders]
        for order in orders:
            agg.append_brand(order.manufacturer)
            agg.append_size(order.size)
            agg.append_color(order.rs_colour)
            agg.append_gender(order.gender)
            agg.append_product_type(order.product_size_attribute)
        return agg
