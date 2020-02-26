import requests
from typing import Optional
from elasticsearch import Elasticsearch, RequestsHttpConnection
from chalicelib.settings import settings


class ElasticRequestException(Exception):
    pass


# @todo : refactoring AlreadyExist, NotExisted, IndexDoesNotExist, ... exceptions


class Elastic:
    __index_name: str = None
    __doc_type: str = None

    def __init__(
        self,
        index_name: str,
        doc_type: str,
        host: Optional[str] = None,
        scroll_lifetime: Optional[str] = None
    ):
        self.__index_name = index_name
        self.__doc_type = doc_type
        self.__host = host or settings.AWS_ELASTICSEARCH_ENDPOINT
        self.__scroll_lifetime = scroll_lifetime or settings.AWS_ELASTICSEARCH_SCROLL_LIFETIME

        self.__index_url = self.__host + '/' + self.__index_name + '/' + self.__doc_type
        self.__client = Elasticsearch(
            hosts=[{'host': settings.AWS_ELASTICSEARCH_HOST, 'port': settings.AWS_ELASTICSEARCH_PORT}],
            # http_auth = awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    @property
    def client(self) -> Elasticsearch:
        return self.__client

    @property
    def index_name(self) -> str:
        return self.__index_name

    @property
    def doc_type(self) -> str:
        return self.__doc_type

    def _create_index(self, mapping: dict, config: dict = None):
        if not mapping:
            raise Exception("Blank mapping found.")
        data = {'mappings': mapping}
        if config:
            data.update({'settings': config})
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.__index_url, json=mapping, headers=headers).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def get_data(self, document_id):
        return requests.get(self.__index_url + '/' + document_id).json().get('_source', None)

    # @todo : rename
    def post_search(self, params: dict):
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.__index_url + '/_search', json=params, headers=headers).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def search_all(self, elastic_query: dict):
        """ Attention! Generator is returned! """

        size = 1000
        scroll_lifetime = self.__scroll_lifetime
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.__index_url + '/_search?scroll=' + scroll_lifetime,
            json={
                'query': elastic_query,
                'size': size,
            },
            headers=headers
        ).json()

        scroll_id = response.get('_scroll_id')
        rows = tuple(map(lambda row: row.get('_source'), response.get('hits', {}).get('hits', [])))

        while rows:
            for row in rows:
                yield row

            if len(rows) < size:
                break

            response = requests.post(
                self.__host + '/_search/scroll',
                json={
                    'scroll': scroll_lifetime,
                    'scroll_id': scroll_id,
                },
                headers=headers
            ).json()

            scroll_id = response.get('_scroll_id')
            rows = tuple(map(lambda row: row.get('_source'), response.get('hits', {}).get('hits', [])))

        requests.delete(
            self.__host + '/_search/scroll',
            json={
                'scroll_id': scroll_id
            },
            headers=headers
        )

    def create(self, document_id, document_data):
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.__index_url + "/" + document_id + "/_create",
            json=document_data,
            headers=headers
        ).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def update_data(self, document_id, params: dict):
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.__index_url + "/" + document_id + "/_update",
            json=params,
            headers=headers
        ).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def update_by_query(self, params: dict):
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.__index_url + "/_update_by_query",
            json=params,
            headers=headers
        ).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def delete_by_query(self, params: dict):
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.__index_url + "/_delete_by_query",
            json=params,
            headers=headers
        ).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))
        return response

    def delete_by_id(self, document_id: str) -> None:
        if not isinstance(document_id, str):
            raise TypeError('{} expects {} in {}, but {} ({}) is given!'.format(
                self.delete_by_id.__qualname__,
                str.__qualname__,
                'document_id',
                document_id,
                type(document_id).__qualname__
            ))
        elif not document_id.strip():
            raise ValueError('{} can not work with empty {}'.format(
                self.delete_by_id.__qualname__,
                'document_id'
            ))

        response = requests.delete(self.__index_url + "/" + document_id).json()
        if response.get('error'):
            raise ElasticRequestException('Elastic search error: {}'.format(response))

