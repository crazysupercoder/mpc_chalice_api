import boto3
from chalicelib.libs.core.chalice.request import MPCRequest
from chalice import UnauthorizedError, BadRequestError
from chalicelib.settings import settings
from boto3.dynamodb.conditions import Key


def register_questions(blue_print):
    def __create_user(email, password):#This should be replaced with new authentication system.
        try:
            poolId = settings.AWS_COGNITO_USER_POOL_ID
            region = settings.AWS_COGNITO_DEFAULT_REGION
            session = boto3.Session(region_name=region)
            client = session.client('cognito-idp')

            response = client.admin_create_user(
                UserPoolId = poolId,
                Username = email,
                TemporaryPassword= password,
                UserAttributes = [
                    {
                        "Name": "email",
                        "Value": email
                    },
                    {
                        "Name": "email_verified",
                        "Value": "true"
                    }
                ]
            )
            return response
        except client.exceptions.UsernameExistsException:
            raise Exception('An account with the given email already exisits.')
        except client.exceptions.InvalidParameterException:
            raise Exception('Invalid email address format.')

    def __save_answers_into_real_user(guest_user_pk, real_user_id):
        region = settings.AWS_COGNITO_DEFAULT_REGION
        table_name = settings.AWS_DYNAMODB_CMS_TABLE_NAME
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)

        if not real_user_id:
            raise Exception('real user id is empty.')
        if not guest_user_pk:
            raise Exception('guest user id is empty.')

        real_user_pk = 'PROFILE#' +  real_user_id
        db_response = table.query(
            KeyConditionExpression=Key('pk').eq(guest_user_pk))
        rows = db_response['Items']

        for row in rows:
            table.update_item(Key={
                'pk': real_user_pk,
                'sk': row.get('sk'),
            }, AttributeUpdates={
                'data': {'Value': row.get('data')}
            })

    @blue_print.route('/questions', methods=['GET'], cors=True)
    def get_all() -> list:
        current_user = blue_print.current_request.current_user
        questions = current_user.profile.questions
        questions = sorted(questions, key = lambda i: int(i['number'])) 
        if len(questions) < 9: 
            new_questions = questions
        else:
            new_questions = questions[-4:]
            new_questions.insert(0, questions[0])
        for question in new_questions:
            if question.get('answer') is None:
                question['isAnswered'] = False
            else:
                question['isAnswered'] = True
        return new_questions


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
                answer_body = answer.get('answer', None)
                if answer_body is None or len(answer_body) == 0:
                    raise Exception('The answer is empty')
                number = answer.get('number')
                old_answer = request.current_user.profile.get_question(number).get('answer')
                if old_answer is not None:
                    return {"status": False, "msg": "The question#{} was already answered.".format(number)}

                attribute_value = answer.get('attribute').get('value')
                if attribute_value == 'name' and number == "1":
                    response = request.current_user.profile.save_answer(number, answer_body)
                    request.current_user.profile.add_language_question()
                    request.current_user.profile.add_gender_question()
                elif attribute_value == 'languages':
                    request.current_user.profile.save_answer(number, answer_body)
                    name = request.current_user.profile.get_question(1)['answer']
                    request.current_user.profile.add_shop4_question(name)
                    request.current_user.profile.add_main_category_brand_size_questions(name)
                elif attribute_value == 'shop4':
                    response = request.current_user.profile.save_answer(number, answer_body)
                    if 'Just me' not in answer_body:
                        name = request.current_user.profile.get_question(1)['answer']
                        request.current_user.profile.add_preferences_shop4_other_question(name, 'the others you shop for')
                    elif len(answer_body) == 1 and answer_body[0] == 'Just me':
                        if request.current_user.is_anyonimous:
                            request.current_user.profile.add_save_preferences_question()
                elif attribute_value == 'preferences_shop4_other':
                    response = request.current_user.profile.save_answer(number, answer_body)
                    if 'Yes' in answer_body:
                        if 'the others' in answer.get('question'):
                            shop4list = request.current_user.profile.get_question(4)['answer']
                            if shop4list:
                                request.current_user.profile.add_names_shop4_question(shop4list)
                        elif 'another person' in answer.get('question'):
                            shop4list = ['Someones']
                            request.current_user.profile.add_names_shop4_question(shop4list)
                        else:
                            request.current_user.profile.add_brand_category_size_questions()
                    elif 'No' in answer_body and request.current_user.is_anyonimous:
                        request.current_user.profile.add_save_preferences_question()
                elif attribute_value == 'names_shop4':
                    temp = answer_body
                    new_answer = {}
                    for k, v in temp.items():
                        new_answer[k] = []
                        for _item in v:
                            new_answer[k].append({
                                'name': _item,
                                'question_made': False 
                            })
                    request.current_user.profile.save_answer(number, new_answer)
                    request.current_user.profile.add_brand_category_size_questions()
                elif attribute_value == 'save_preferences':
                    # Real user should be created from guest.
                    email = answer_body.get('email')
                    password = answer_body.get('password')
                    if not email:
                        raise BadRequestError('Please input email!')
                    if not password:
                        raise BadRequestError('Please input password!')
                    response = __create_user(email, password)
                    real_user = response.get('User') or {}
                    new_answer = "Your account was created. You should log in and continue to get R100."
                    request.current_user.profile.save_answer(number, new_answer)
                    __save_answers_into_real_user(request.current_user.profile.get_partition_key(), real_user.get('Username'))
                else:
                    response = request.current_user.profile.save_answer(number, answer_body)
            return {"status": True}
        except Exception as e:
            return {"status": False, "msg": str(e)}
    
    @blue_print.route('/get_credit', methods=['GET'], cors=True)
    def messages_list():
        current_user = blue_print.current_request.current_user
        if current_user.is_anyonimous:
            raise UnauthorizedError('Authentication is required!')
        else:
            # Do something
            # Give R100 to user.
            pass

    @blue_print.route('/questions/answer', methods=['PUT'], cors=True)
    def answer():
        request: MPCRequest = blue_print.current_request
        try:
            params = request.json_body
            if isinstance(params, list):
                answers = params
            else:
                answers = [params]
            for answer in answers:
                answer_body = answer.get('answer', None)
                if answer_body is None or len(answer_body) == 0:
                    raise Exception('The answer is empty')
                number = answer.get('number')
                question = request.current_user.profile.get_question(number)
                attribute_value = answer.get('attribute').get('value')
                if number == "1":
                    request.current_user.profile.save_answer(number, answer_body)
                    request.current_user.profile.update_question_name(answer_body)
                elif attribute_value == 'names_shop4':
                    temp = answer_body
                    new_answer = {}
                    for k, v in temp.items():
                        new_answer[k] = []
                        for _item in v:
                            new_answer[k].append({
                                'name': _item,
                                'question_made': True 
                            })
                    request.current_user.profile.save_answer(number, new_answer)
                else:
                    request.current_user.profile.save_answer(number, answer_body)
            return {"status": True}
        except Exception as e:
            return {"status": False, "msg": str(e)}
