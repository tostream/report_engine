from typing import Optional
import base64, logging, requests
from requests import Request, Session, Response


class BaseApi:
    def __init__(self, 
                base_url: str,
                user: Optional[str] =None,
                password: Optional[str] =None,
                auth: Optional[str] = 'basic'):
        self.base_url = base_url
        self.user = user
        self.password = password
        if auth == 'basic':
            self.auth = 'Basic ' + str(base64.b64encode((self.user + ":" + self.password).encode("utf-8")).decode('utf-8'))
        elif auth == 'bearer':
            self.auth = 'Bearer ' + str(password)

        self.headers = {
            'Authorization': self.auth,
            'Content-Type': 'application/json'
        }

    def _make_request(self, req: Request) -> Optional[Response]:
        """_summary_

        Args:
            req (Request): _description_

        Returns:
            Optional[Response]: _description_
        """
        response = None

        try:
            s = Session()
            prepped = prepped = req.prepare()
            response = s.send(prepped)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            logging.error(f'Exception querying api "{req.url}"')
            logging.debug(str(type(err)))
            logging.debug(err)
            print(f'Exception querying api "{req.url}"')
        except requests.exceptions.HTTPError as errh:
            logging.error(f'Http error querying api "{req.url}"')
            logging.debug(str(type(err)))
            logging.debug(err)
            print(f'Http error querying api "{req.url}"')
        except requests.exceptions.ConnectionError as errc:
            logging.error(f'Connection error querying api "{req.url}"')
            logging.debug(str(type(err)))
            logging.debug(err)
            print(f'Connection error querying api "{req.url}"')
        except requests.exceptions.Timeout as errt:
            logging.error(f'Timeout error querying api "{req.url}"')
            logging.debug(str(type(err)))
            logging.debug(err)
            print(f'Timeout error querying api "{req.url}"')
        
        return response
        

    def _get(self, param:str, header:dict, **kwargs: any) -> Optional[Response]:
        """_summary_

        Args:
            param (str): _description_
            header (dict): _description_

        Returns:
            Optional[Response]: _description_
        """
        return self._make_request(Request('GET', self.base_url + param, headers=header, **kwargs))

    def _post(self, param:str, header:dict,**kwargs: any) -> Optional[Response]:
        """_summary_

        Args:
            param (str): _description_
            header (dict): _description_
            body (dict): _description_

        Returns:
            Optional[Response]: _description_
        """
        return self._make_request(Request('POST', self.base_url + param, headers=header, **kwargs))

    def _patch(self, param:str, header:dict, body:dict, **kwargs: any) -> Optional[Response]:
        """_summary_

        Args:
            param (str): _description_
            header (dict): _description_
            body (dict): _description_

        Returns:
            Optional[Response]: _description_
        """
        return self._request_handler(Request('PATCH', self.base_url + param, headers=header, json=body, **kwargs))

    def _put(self, param:str, header:dict, **kwargs: any) -> Optional[Response]:
        """_summary_

        Args:
            param (str): _description_
            header (dict): _description_

        Returns:
            Optional[Response]: _description_
        """
        return self._request_handler(Request('PUT', self.base_url + param, headers=header, **kwargs))
