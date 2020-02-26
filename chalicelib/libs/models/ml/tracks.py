from typing import List


class UserTrackEntry(object):
    __genders: List[str] = []
    __product_types: List[str] = []
    __product_sub_types: List[str] = []
    __brands: List[str] = []
    __sizes: List[str] = []
    __product_count: int = 500

    @property
    def product_count(self) -> int:
        return self.__product_count

    @product_count.setter
    def product_count(self, value: int):
        self.__product_count = value

    @property
    def genders(self) -> List[str]:
        return self.__genders
    
    @genders.setter
    def genders(self, value: List[str]):
        if value is None:
            value = []
        self.__genders = [str(item).lower() for item in value]

    @property
    def gender_score(self) -> float:
        if len(self.genders) > 0:
            return self.product_count / len(self.genders)
        else:
            return 0.0

    @property
    def product_types(self):
        return self.__product_types
    
    @product_types.setter
    def product_types(self, value: List[str]):
        if value is None:
            value = []
        self.__product_types = [str(item).lower() for item in value]

    @property
    def product_type_score(self) -> float:
        if len(self.product_types) > 0:
            return self.product_count / len(self.product_types)
        else:
            return 0.0

    @property
    def product_sub_types(self):
        return self.__product_sub_types
    
    @product_sub_types.setter
    def product_sub_types(self, value: List[str]):
        if value is None:
            value = []
        self.__product_sub_types = [str(item).lower() for item in value]

    @property
    def product_sub_type_score(self) -> float:
        if len(self.product_sub_types) > 0:
            return self.product_count / len(self.product_sub_types)
        else:
            return 0.0

    @property
    def brands(self):
        return self.__brands
    
    @brands.setter
    def brands(self, value: List[str]):
        if value is None:
            value = []
        self.__brands = [str(item).lower() for item in value]

    @property
    def brand_score(self) -> float:
        if len(self.brands) > 0:
            return self.product_count / len(self.brands)
        else:
            return 0.0

    @property
    def sizes(self):
        return self.__sizes
    
    @sizes.setter
    def sizes(self, value: List[str]):
        if value is None:
            value = []
        self.__sizes = [str(item).lower() for item in value]

    @property
    def size_score(self) -> float:
        if len(self.sizes) > 0:
            return self.product_count / len(self.sizes)
        else:
            return 0.0

    def __init__(
        self,
        product_count: int = 500,
        genders: List[str] = [],
        product_types: List[str] = [],
        product_sub_types: List[str] = [],
        brands: List[str] = [],
        sizes: List[str] = [],
    ):
        self.product_count = max(
            product_count if product_count else 1, 1)
        self.genders = genders
        self.product_types = product_types
        self.product_sub_types = product_sub_types
        self.brands = brands
        self.sizes = sizes
