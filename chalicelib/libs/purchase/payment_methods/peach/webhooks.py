import json
import binascii
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from chalicelib.extensions import *
from chalicelib.settings import settings
from chalicelib.libs.core.logger import Logger


# ----------------------------------------------------------------------------------------------------------------------


class WebhooksDecryptor(object):
    def __init__(self):
        self.__decryption_key = settings.PEACH_PAYMENT_WEBHOOKS_DECRYPTION_KEY

    def decrypt(self, initialization_vector: str, auth_tag: str, encrypted_data: str) -> dict:
        if not isinstance(initialization_vector, str):
            raise ArgumentTypeException(self.decrypt, 'initialization_vector', initialization_vector)
        elif not initialization_vector:
            raise ArgumentCannotBeEmptyException(self.decrypt, 'initialization_vector')

        if not isinstance(auth_tag, str):
            raise ArgumentTypeException(self.decrypt, 'auth_tag', auth_tag)
        elif not auth_tag:
            raise ArgumentCannotBeEmptyException(self.decrypt, 'auth_tag')

        if not isinstance(encrypted_data, str):
            raise ArgumentTypeException(self.decrypt, 'encrypted_data', encrypted_data)
        elif not encrypted_data:
            raise ArgumentCannotBeEmptyException(self.decrypt, 'encrypted_data')

        key = binascii.unhexlify(self.__decryption_key)
        iv = binascii.unhexlify(initialization_vector)
        auth_tag = binascii.unhexlify(auth_tag)
        cipher_text = binascii.unhexlify(encrypted_data)
        decryptor = Cipher(algorithms.AES(key), modes.GCM(iv, auth_tag), backend=default_backend()).decryptor()
        data = decryptor.update(cipher_text) + decryptor.finalize()
        data = json.loads(data)

        return data


# ----------------------------------------------------------------------------------------------------------------------


class WebhooksFlowLog(object):
    def __init__(self, flow_id: str):
        if not isinstance(flow_id, str):
            raise ArgumentTypeException(self.__init__, 'flow_id', flow_id)
        elif not flow_id.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'flow_id')

        self.__flow_id = flow_id
        self.__logger = Logger()

    def write(self, message: str) -> None:
        self.__logger.log_simple('Peach Payment Webhooks Log #{}: {}'.format(
            self.__flow_id,
            message
        ))


# ----------------------------------------------------------------------------------------------------------------------

