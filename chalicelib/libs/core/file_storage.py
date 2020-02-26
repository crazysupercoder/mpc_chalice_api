import os
import shutil
import re
import boto3
from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import settings


# ----------------------------------------------------------------------------------------------------------------------


class FileStorageFile(object):
    def __init__(self, key: str, url: str):
        required = {
            'key': key,
            'url': url,
        }
        for param_name in tuple(required.keys()):
            if not isinstance(required[param_name], str):
                raise ArgumentTypeException(self.__init__, param_name, required[param_name])
            elif not str(required[param_name]).strip():
                raise ArgumentCannotBeEmptyException(self.__init__, param_name)

        self.__key = key
        self.__url = url

    @property
    def key(self) -> str:
        return self.__key

    @property
    def url(self) -> str:
        return self.__url


# ----------------------------------------------------------------------------------------------------------------------


class FileStorageInterface(object):
    def upload(self, source_path: str, destination_key: str) -> FileStorageFile:
        raise NotImplementedError()

    def get(self, key: str) -> Optional[FileStorageFile]:
        raise NotImplementedError()


# ----------------------------------------------------------------------------------------------------------------------


class _FileLocalStorage(FileStorageInterface):
    def __init__(self, root_path: str, root_url: str):
        if not isinstance(root_path, str):
            raise ArgumentTypeException(self.__init__, 'root_path', root_path)
        elif not root_path.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'root_path')
        elif not os.path.isdir(root_path):
            raise ArgumentValueException('{} can\'t be initiated: {} is not a dir!'.format(
                self.__init__.__qualname__,
                root_path
            ))

        if not isinstance(root_url, str):
            raise ArgumentTypeException(self.__init__, 'root_url', root_url)
        elif not root_url.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'root_url')
        elif not re.match(re.compile(
            # https://fooobar.com/questions/114232/python-how-to-validate-a-url-in-python-malformed-or-not
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE), root_url
        ):
            raise ArgumentValueException('{} can\'t be initiated: {} is not a url!'.format(
                self.__init__.__qualname__,
                root_url
            ))

        self.__root_path = root_path
        self.__root_url = root_url

    def __get_path(self, key: str) -> str:
        return self.__root_path + key

    def __get_url(self, key: str) -> str:
        return self.__root_url + key

    def upload(self, source_path: str, destination_key: str) -> FileStorageFile:
        required = {
            'source_path': source_path,
            'destination_key': destination_key,
        }
        for param_name in tuple(required.keys()):
            if not isinstance(required[param_name], str):
                raise ArgumentTypeException(self.__init__, param_name, required[param_name])
            elif not str(required[param_name]).strip():
                raise ArgumentCannotBeEmptyException(self.__init__, param_name)

        destination_path = self.__get_path(destination_key)
        destination_url = self.__get_url(destination_key)

        # @todo : check path and create dirs
        shutil.copy(source_path, destination_path)

        return FileStorageFile(destination_key, destination_url)

    def get(self, key: str) -> Optional[FileStorageFile]:
        if not isinstance(key, str):
            raise ArgumentTypeException(self.__init__, 'destination_key', key)
        elif not str(key).strip():
            raise ArgumentCannotBeEmptyException(self.__init__, key)

        destination_path = self.__get_path(key)
        if not os.path.isfile(destination_path):
            return None

        destination_url = self.__get_url(key)
        return FileStorageFile(key, destination_url)


# ----------------------------------------------------------------------------------------------------------------------


class _AwsS3FileStorage(FileStorageInterface):
    def __init__(self, bucket: str, root_url: str):
        if not isinstance(bucket, str):
            raise ArgumentTypeException(self.__init__, 'bucket', bucket)
        elif not bucket.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'bucket')

        if not isinstance(root_url, str):
            raise ArgumentTypeException(self.__init__, 'root_url', root_url)
        elif not root_url.strip():
            raise ArgumentCannotBeEmptyException(self.__init__, 'root_url')
        elif not re.match(re.compile(
            # https://fooobar.com/questions/114232/python-how-to-validate-a-url-in-python-malformed-or-not
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE), root_url
        ):
            raise ArgumentValueException('{} can\'t be initiated: {} is not a url!'.format(
                self.__init__.__qualname__,
                root_url
            ))

        self.__bucket = bucket
        self.__root_url = root_url
        self.__client = boto3.client('s3')

    def upload(self, source_path: str, destination_key: str) -> FileStorageFile:
        self.__client.put_object(
            ACL='public-read',
            Body=open(source_path, 'rb').read(),
            Bucket=self.__bucket,
            Key=destination_key,
        )

        return FileStorageFile(
            destination_key,
            self.__get_url(destination_key)
        )

    def get(self, key: str) -> Optional[FileStorageFile]:
        if not isinstance(key, str):
            raise ArgumentTypeException(self.get, 'key', key)
        elif not key.strip():
            raise ArgumentCannotBeEmptyException(self.get, 'key')

        results = self.__client.list_objects(Bucket=self.__bucket, Prefix=key)
        if 'Contents' not in results:
            return None

        return FileStorageFile(
            key,
            self.__get_url(key)
        )

    def __get_url(self, key) -> str:
        return self.__root_url + key


# ----------------------------------------------------------------------------------------------------------------------


class FileStorageImplementation(FileStorageInterface):
    def __init__(self) -> None:
        class_name = settings.FILE_STORAGE_CONFIG.get('class')
        class_params = settings.FILE_STORAGE_CONFIG.get('params')
        self.__storage: FileStorageInterface = create_object(class_name, class_params)

    def upload(self, source_path: str, destination_key: str) -> FileStorageFile:
        return self.__storage.upload(source_path, destination_key)

    def get(self, key: str) -> Optional[FileStorageFile]:
        return self.__storage.get(key)


# ----------------------------------------------------------------------------------------------------------------------

