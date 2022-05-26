
import base64
import logging
import requests


class BaseApi:
    def __init__(self, base_url, user, password):
        self.base_url = base_url
        self.user = user
        self.password = password
        self.auth = 'Basic ' + str(base64.b64encode((self.user + ":" + self.password).encode("utf-8")).decode('utf-8'))
        self.header = {
            'Authorization': self.auth,
            'Content-Type': 'application/json'
        }

    def _get_query_with_retry(self, param, header=None):
        retry = 0
        response = None

        while retry != 5:
            try:
                url = self.base_url + param
                response = requests.get(url, headers=header, timeout=10)
                break
            except Exception as e:
                logging.debug(f'Exception querying api "{self.base_url}"')
                logging.debug(str(type(e)))
                logging.debug(e)
            retry += 1

        if response is None:
            logging.warning(f'Can\'t querying api "{self.base_url}" : fail after 5 retries.')

        return response

    def _post_query_with_retry(self, param, header=None, body=None):
        retry = 0
        response = None

        while retry != 5:
            try:
                url = self.base_url + param
                response = requests.post(url, headers=header, json=body, timeout=10)
                break
            except Exception as e:
                logging.debug(f'Exception querying api "{self.base_url}"')
                logging.debug(str(type(e)))
                logging.debug(e)
            retry += 1

        if response is None:
            logging.warning(f'Can\'t querying api "{self.base_url}" : fail after 5 retries.')

        return response

    def _patch_query_with_retry(self, param, header=None, body=None):
        retry = 0
        response = None

        while retry != 5:
            try:
                url = self.base_url + param
                response = requests.patch(url=url, headers=header, json=body, timeout=10)
                break
            except Exception as e:
                logging.debug(f'Exception querying api "{self.base_url}"')
                logging.debug(str(type(e)))
                logging.debug(e)
            retry += 1

        if response is None:
            logging.warning(f'Can\'t querying api "{self.base_url}" : fail after 5 retries.')

        return response

    def _put_query_with_retry(self, param, header=None):
        retry = 0
        response = None

        while retry != 5:
            try:
                url = self.base_url + param
                response = requests.put(url, headers=header, timeout=10)
                break
            except Exception as e:
                logging.debug(f'Exception querying api "{self.base_url}"')
                logging.debug(str(type(e)))
                logging.debug(e)
            retry += 1

        if response is None:
            logging.warning(f'Can\'t querying api "{self.base_url}" : fail after 5 retries.')

        return response
