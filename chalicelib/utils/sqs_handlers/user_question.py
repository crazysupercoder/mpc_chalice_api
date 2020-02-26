from .base import *
from chalicelib.libs.models.mpc.Cms.UserQuestions import *


class UserQuestionSqsHandler(SqsHandlerInterface):
    def __init__(self):
        self.__model = UserQuestionModel()

    def handle(self, sqs_message: SqsMessage) -> None:
        if int(sqs_message.message_data.get('is_deleted')) == 1:
            self.__model.delete(sqs_message.message_data.get('id'))
        else:
            data = sqs_message.message_data
            question = UserQuestionEntity(
                str(data.get('id')),
                str(data.get('type')),
                str(data.get('question')),
                int(data.get('priority')),
                str(data.get('attribute', {}).get('type')),
                str(data.get('attribute', {}).get('value')),
                tuple(map(lambda option: UserQuestionEntityOption(
                    str(option.get('id')),
                    str(option.get('type')),
                    str(option.get('value')),
                    str(option.get('png_image')),
                    str(option.get('svg_image')),
                    str(option.get('entity')),
                    str(option.get('entity_id')),
                    str(option.get('fixel_category_id')),
                    str(option.get('fixel_category_name')),
                    str(option.get('brand_id')),
                    str(option.get('brand_name')),
                    tuple(map(lambda suboption: UserQuestionEntitySubOption(
                        str(suboption.get('id')),
                        str(suboption.get('type')),
                        str(suboption.get('value')),
                        str(suboption.get('png_image')),
                        str(suboption.get('svg_image')),
                        str(suboption.get('entity')),
                        str(suboption.get('entity_id')),
                        str(suboption.get('fixel_category_id')),
                        str(suboption.get('fixel_category_name')),
                        str(suboption.get('brand_id')),
                        str(suboption.get('brand_name')),
                    ), option.get('suboptions'))) if option.get('suboptions') else None
                ), data.get('options'))) if data.get('options') else None
            )
            self.__model.save(question)

