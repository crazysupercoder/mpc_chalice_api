from chalice import Blueprint, NotFoundError
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.purchase.core import ReturnRequest
from chalicelib.libs.purchase.returns.storage import ReturnRequestStorageImplementation


def register_returns(blueprint: Blueprint) -> None:
    def __check_header_or_error():
        read_api_header_value = blueprint.current_request.headers.get(settings.READ_API_HEADER_NAME)
        if read_api_header_value != settings.READ_API_HEADER_VALUE:
            raise HttpIncorrectInputDataException('Authentication is missed!')

    # ------------------------------------------------------------------------------------------------------------------
    #                                               RETURN REQUEST INFO
    # ------------------------------------------------------------------------------------------------------------------

    @blueprint.route('/returns/info/{request_number}', methods=['POST'], cors=True)
    def info(request_number):
        __check_header_or_error()

        returns_storage = ReturnRequestStorageImplementation()
        return_request = returns_storage.load(ReturnRequest.Number(request_number))
        if not return_request:
            raise NotFoundError('Return Request does not exist!')

        return {
            'number': return_request.number.value,
            'requested_at': return_request.requested_at.strftime('%Y-%m-%d %H:%M:%S'),
            'items': [{
                'order_number': item.order_number.value,
                'simple_sku': item.simple_sku.value,
                'qty': item.qty.value,
                'cost': item.cost.value,
                'reason': {
                    'descriptor': item.reason.descriptor,
                    'label': item.reason.label,
                },
                'attached_files': [file.url for file in item.attached_files],
                'additional_comment': item.additional_comment.value if item.additional_comment else None,
                'status': {
                    'descriptor': item.status.value,
                    'label': item.status.label,
                },
            } for item in return_request.items],
            'delivery_method': {
                'descriptor': return_request.delivery_method.descriptor,
                'label': return_request.delivery_method.label,
            },
            'refund_method': {
                'descriptor': return_request.refund_method.descriptor,
                'label': return_request.refund_method.label,
                'extra_data': return_request.refund_method.extra_data,
            }
        }

