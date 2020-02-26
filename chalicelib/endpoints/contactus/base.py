from chalice import Blueprint
from chalicelib.extensions import *
from chalicelib.libs.contactus.sqs import ContactusSqsSenderEvent, ContactusRequest
from chalicelib.libs.core.sqs_sender import SqsSenderImplementation
from chalicelib.libs.core.file_storage import FileStorageImplementation
import os
import uuid
import hashlib

contactus_blueprint = Blueprint(__name__)
    
@contactus_blueprint.route('/contact', methods=['POST'], cors=True)
def contactus_cash_out():
    file_storage = FileStorageImplementation()

    try:
        request_data = contactus_blueprint.current_request.json_body

        issue = request_data.get('issue', '')
        if not issue:
            raise ValueError('Please select issue')

        issue_detail = request_data.get('issue_detail', '')
        if not issue_detail:
            raise ValueError('Please select issue in detail')

        first_name = request_data.get('first_name', '')
        if not first_name:
            raise ValueError('Please input first name')

        last_name = request_data.get('last_name', '')
        if not last_name:
            raise ValueError('Please input last name')

        phone = request_data.get('phone', '')
        if not phone:
            raise ValueError('Please input phone number')

        email = request_data.get('email', '')
        if not email:
            raise ValueError('Please input email')

        subject = request_data.get('subject', '')
        if not subject:
            raise ValueError('Please input subject')

        message = request_data.get('message', '')

        file_id = request_data.get('file_id', '')
        file = file_storage.get(file_id) if file_id else None
        file_url = file.url if file else ''

        contactus_request = ContactusRequest(
            issue, issue_detail, first_name, last_name, phone, email, subject, message, file_url
        )
        sqs_sender = SqsSenderImplementation()


        event = ContactusSqsSenderEvent(contactus_request)
        sqs_sender.send(event)
        return {'status': True}
    except BaseException as e:
        return {
            'status': False,
            'message': str(e)
        }

# ------------------------------------------------------------------------------------------------------------------
#                                                   UPLOAD
# ------------------------------------------------------------------------------------------------------------------

@contactus_blueprint.route(
    '/upload',
    methods=['POST'], cors=True, content_types=['application/octet-stream']
)
def contact_upload():
    file_storage = FileStorageImplementation()

    try:
        max_size_mb = 4
        size_in_bytes = len(contactus_blueprint.current_request.raw_body)
        if size_in_bytes > max_size_mb * 1024 * 1024:
            raise HttpIncorrectInputDataException('Uploaded file max size is {} Mb!'.format(max_size_mb))
        elif not size_in_bytes:
            raise HttpIncorrectInputDataException('Uploaded file cannot be empty!')

        file_id = str(uuid.uuid4())
        file_id = hashlib.md5(file_id.encode('utf-8')).hexdigest()
        file_content = contactus_blueprint.current_request.raw_body

        # save tmp file
        tmp_file_path = '/tmp/' + file_id
        with open(tmp_file_path, 'wb') as tmp_file:
            tmp_file.write(file_content)

        # check tmp file
        import fleep
        with open(tmp_file_path, 'rb') as tmp_file:
            file_info = fleep.get(tmp_file.read(128))

        types_map = {
            'image/png': 'png',
            'image/jpeg': 'jpg',
            'image/pjpeg': 'jpg',
            'image/bmp': 'bmp',
            'image/x-windows-bmp': 'bmp',
            'image/gif': 'gif',
            'application/pdf': 'pdf',
        }
        if not file_info.mime or file_info.mime[0] not in types_map.keys():
            raise HttpIncorrectInputDataException('Mime-type is not supported!')

        # upload
        extension = types_map[file_info.mime[0]]
        destination_key = file_id + '.' + extension
        file_storage.upload(tmp_file_path, destination_key)

        # remove tmp file
        if os.path.isfile(tmp_file_path):
            os.remove(tmp_file_path)

        return {
            'key': destination_key,
        }
    except BaseException as e:
        return http_response_exception_or_throw(e)
