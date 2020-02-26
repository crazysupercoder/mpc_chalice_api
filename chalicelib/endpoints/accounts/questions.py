from chalicelib.libs.core.chalice.request import MPCRequest


def register_questions(blue_print):
    @blue_print.route('/questions', methods=['GET'], cors=True)
    def get_all() -> list:
        current_user = blue_print.current_request.current_user
        questions = current_user.profile.questions
        res = []
        for question in questions:
            if question.get('answer') is None:
                res.append(question)
        return res


    @blue_print.route('/questions/{number}', methods=['GET'], cors=True)
    def get_question(number):
        current_user = blue_print.current_request.current_user
        item = current_user.profile.get_question(number)
        return item


    @blue_print.route('/questions/answer', methods=['POST'], cors=True)  # , authorizer=iam_authorizer)
    def answer():
        request: MPCRequest = blue_print.current_request
        try:
            params = request.json_body
            if isinstance(params, list):
                answers = params
            else:
                answers = [params]
            for answer in answers:
                number = answer.get('number')
                response = request.current_user.profile.save_answer(number, answer.get('answer'))

                attribute_value = answer.get('attribute').get('value')
                if attribute_value == 'name' and number == "1":
                    name = answer.get('answer')
                    request.current_user.profile.add_language_question()
                    request.current_user.profile.add_gender_question()
                    request.current_user.profile.add_shop4_question(name)
                    request.current_user.profile.add_category_question(name)
                    request.current_user.profile.add_brand_question(name)
                    request.current_user.profile.add_size_question(name)
                    request.current_user.profile.add_preferences_shop4_other_question('the others you shop for')
                elif attribute_value == 'preferences_shop4_other':
                    if 'Yes' in answer.get('answer') and 'the others' in answer.get('question'):
                        shop4list = request.current_user.profile.get_question(4)['answer']
                        if shop4list:
                            request.current_user.profile.add_names_shop4_question(shop4list)
                    elif 'No' in answer.get('answer'):
                        request.current_user.profile.add_save_preferences_question()
                elif attribute_value == 'names_shop4':
                    temp = answer.get('answer')
                    names = []
                    for item in temp.values():
                        for _item in item:
                            names.append(_item)
                    request.current_user.profile.add_brand_category_size_questions(names)
            return {"status": True}
        except Exception as e:
            return {"status": False, "msg": str(e)}
