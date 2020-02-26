from typing import List
from decimal import Decimal


TARGET_ATTR_KEY_MAPPING = {
    'product.brand': 'manufacturer',
    'customer.gender': 'gender',
    'product.producttype': 'product_size_attribute',
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
    __answers: List[str]
    __target_attr: str
    __options: List[AnswerOption]
    product_count: float = 500

    def __init__(
            self,
            answer: List[Decimal],
            attribute: dict,
            options: List[dict],
            product_count: int = 500,
            **kwargs):
        self.target_attr = attribute
        self.options = options
        if len(answer) == 0:
            raise Exception("Blank answer is not permitted.")
        else:
            self.answers = answer
        self.product_count = product_count

    @property
    def options(self) -> List[AnswerOption]:
        return self.__options

    @options.setter
    def options(self, items: List[dict]):
        self.__options = [AnswerOption(**item) for item in items]

    @property
    def answers(self):
        return self.__answers

    @answers.setter
    def answers(self, value: List[Decimal]):
        if len(value) > 0 and type(value[0]) is Decimal:
            self.__answers = [option.value
                for option in self.options
                if option.id in [int(answer) for answer in value]]
            for idx, answer in enumerate(self.__answers):
                if str(answer).lower() in ['male', 'males', 'mens']:
                    self.__answers[idx] = 'MENS'
                elif str(answer).lower() in ['female', 'ladies', 'lady']:
                    self.__answers[idx] = 'LADIES'
        else:
            self.__answers = value

    @property
    def target_attr(self):
        return self.__target_attr

    @target_attr.setter
    def target_attr(self, value: dict):
        key = "%s.%s" % (
            value.get('type', ''),
            value.get('value', '')
        )
        self.__target_attr = TARGET_ATTR_KEY_MAPPING.get(key)

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
