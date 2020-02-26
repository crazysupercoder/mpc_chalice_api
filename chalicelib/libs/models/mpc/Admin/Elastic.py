import requests
import json
import os


class Elastic:
    def __init__(self, auth=None):
        self.__auth = auth

    def recreate(self, index_name="mpc-index", host="http://localhost:9200"):
        mapping_url = host + '/' + index_name

        dir_path = os.path.dirname(os.path.realpath(__file__))
        fp = open("{}/Config/elastic.json".format(dir_path))
        mapping = json.load(fp)
        headers = {"Content-Type": "application/json"}

        if self.__auth:
            print("running authed")
            requests.delete(mapping_url, auth=self.__auth)
            res = requests.put(mapping_url, json=mapping, headers=headers, auth=self.__auth)
        else:
            print("not authed")
            requests.delete(mapping_url)
            res = requests.put(mapping_url, json=mapping, headers=headers)

        print(res)
