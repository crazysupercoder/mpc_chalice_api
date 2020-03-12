from decimal import Decimal
from typing import List, Union, Dict, Optional
from warnings import warn
from chalicelib.settings import settings


TARGET_ATTR_KEY_MAPPING = {
    'product.brand': 'manufacturer',
    'customer.gender': 'gender',
    'product.producttype': 'product_size_attribute',
}


GENDER_VALUE_MAPPING = {
    'men': 'MENS',
    'mens': 'MENS',
    'women': 'LADIES',
    'lady': 'LADIES',
    'ladies': 'LADIES',
    'kid': 'KIDS',
    'kids': 'KIDS',
}


class AnswerOption(object):
    id: int
    type: str
    value: str

    def __init__(
            self,
            id: Decimal,
            type: str,
            value: str,
            **kwargs):
        self.id = int(id)
        self.type = type
        self.value = str(value)


class Answer(object):
    __answers: Union[List[str], Dict[str, List[str]]]
    __target_attr: str
    __options: List[AnswerOption]
    __queries: List[Dict[str, List[str]]] = []
    product_count: float = 500

    def __init__(
            self,
            answer: Union[List[Union[str, Decimal]], Dict[str, List[str]]],
            attribute: dict,
            # options: List[dict],
            product_count: int = 500,
            **kwargs):
        self.__queries = []
        self.target_attr = attribute
        # self.options = options
        self.answers = answer
        self.product_count = product_count

    def __handle_exception(self, str):
        if settings.DEBUG:
            raise Exception(str)
        else:
            warn(str)

    @property
    def options(self) -> List[AnswerOption]:
        return self.__options

    @options.setter
    def options(self, items: List[dict]):
        self.__options = [AnswerOption(**item) for item in items]

    @property
    def queries(self) -> List[Dict[str, List[str]]]:
        return self.__queries

    @property
    def answers(self):
        return self.__answers

    @answers.setter
    def answers(self, value: Union[List[str], Dict[str, List[str]]]):
        self.__answers = value

        # Analyzing target attributes
        target_attr_type = self.target_attr.get('type')
        target_attr_value = self.target_attr.get('value')
        if isinstance(value, list) and all(isinstance(x, (str, Decimal)) for x in value):
            # NOTE: is there any better? This is coding challenge.
            value = [str(item) for item in value]
            if target_attr_type == 'product':
                if target_attr_value == 'brand':
                    self.__queries.append({'brand_code': [str(item).strip().lower() for item in value]})
                else:
                    self.__handle_exception("Uknown case here...")
            elif target_attr_type == 'customer':
                if target_attr_value == 'shop4':
                    self.__queries.append({
                        'gender': [GENDER_VALUE_MAPPING.get(str(item).lower()) for item in value if item]
                    })
            else:
                self.__handle_exception("Uknown case here...")
        elif isinstance(value, dict):
            if target_attr_type == 'product':
                if target_attr_value == 'category':
                    for product_type, sub_types in value.items():
                        if isinstance(sub_types, list) and all(isinstance(x, str) for x in sub_types):
                            self.__queries.append({
                                'product_size_attribute': [product_type],
                                'rs_product_sub_type': sub_types,
                            })
                        elif isinstance(sub_types, str):
                            self.__queries.append({
                                'product_size_attribute': [product_type],
                                'rs_product_sub_type': [sub_types],
                            })
                        else:
                            self.__handle_exception("Unknown case found here.")
                elif target_attr_value == 'size':
                    for product_type, sizes in value.items():
                        if isinstance(sizes, list) and all(isinstance(x, str) for x in sizes):
                            self.__queries.append({
                                'product_size_attribute': [product_type],
                                'sizes': sizes,
                            })
                        elif isinstance(sizes, str):
                            self.__queries.append({
                                'product_size_attribute': [product_type],
                                'sizes': [sizes],
                            })
                        else:
                            self.__handle_exception("Unknown case found here.")
                else:
                    self.__handle_exception("Unknown case found here.")
            else:
                self.__handle_exception("Unknown case found here.")
        else:
            self.__handle_exception("Unknown answer type %s" % value)

    @property
    def target_attr(self):
        return self.__target_attr

    @target_attr.setter
    def target_attr(self, value: dict):
        # key = "%s.%s" % (
        #     value.get('type', ''),
        #     value.get('value', '')
        # )
        # self.__target_attr = TARGET_ATTR_KEY_MAPPING.get(key)
        self.__target_attr = value

    @classmethod
    def load_from_json_file(
            cls,
            filename: str = 'questions.json',
            product_count: int = 500):
        items = load_questions_from_json_file(filename=filename)
        return [cls(product_count=product_count, **item) for item in items]

    @property
    def total_answers(self) -> int:
        return max(len(self.answers), 1)

    @property
    def question_score(self) -> float:
        return self.product_count / self.total_answers
