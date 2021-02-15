import json
import requests

from datetime import datetime
from logging import getlogger

logger = getLogger()


class QuantumLeapClientException(Exception):
    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class Client(object):
    version = "v2"

    def __init__(self, host='localhost', port=8668,
                 service=None, service_path=None):
        self.host = host
        self.port = port
        self.base_url = f'http://{host}:{port}/{self.version}'
        self.service = service
        self.service_path = service_path
        pass

    def _do_request(self, method=None, url=None,
                    queries=None, body=None, headers=None):
        if method == 'POST':
            response = requests.post(url, params=queries,
                                     data=json.dumps(body),
                                     headers=headers)
            if response.status_code == 200:
                return{"status": "Notification successfully processed"}
            elif response.status_code == 201:
                return{"status": "Notification successfully processed"}
            elif response.status_code == 400:
                return{"status": "Received notification is not valid"}
            elif response.status_code == 500:
                return{"status": "Internal server error"}
            else:
                return{"status": "Error"}
        elif method == 'DELETE':
            response = requests.delete(url, params=queries)
            if response.status_code == 204:
                return {"status": "delete historical records."}
            else:
                return {"status": "Error"}
        else:
            response = requests.get(url, params=queries)
        return response.json()

    def wrap_params(self, queries):
        params = {}
        if 'type' in queries:
            params['type'] = queries['type']
        if 'aggrMethod' in queries:
            params['aggrMethod'] = queries['aggrMethod']
        if 'aggrPeriod' in queries:
            params['aggrPeriod'] = queries['aggrPeriod']
        if 'options' in queries:
            params['options'] = queries['options']
        if 'fromDate' in queries:
            fromDate = queries['fromDate']
            if isinstance(fromDate, datetime):
                self.fromDate = fromdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['fromDate'] = self.fromDate
            else:
                params['fromDate'] = queries['fromDate']
        if 'toDate' in queries:
            toDate = queries['toDate']
            if isinstance(toDate, datetime):
                self.toDate = toDate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['toDate'] = self.toDate
            else:
                params['toDate'] = queries['toDate']
        if 'lastN' in queries:
            params['lastN'] = queries['lastN']
        if 'limit' in queries:
            params['limit'] = queries['limit']
        if 'offset' in queries:
            params['offset'] = queries['offset']
        if 'georel' in queries:
            params['georel'] = queries['georel']
        if 'geometory' in queries:
            params['geometory'] = queries['geometory']
        if 'coords' in queries:
            params['coords'] = queries['coords']
        if 'id' in queries:
            params['id'] = queries['id']
        if 'aggrScope' in queries:
            params['aggrScope'] = queries['aggrScope']
        return params

    def wrap_subscribe_params(self, queries):
        params = {}
        if 'orionUrl' in queries:
            params['orionUrl'] = queries['orionUrl']
        else:
            params['orionUrl'] = 'http://localhost:1026/v2'
        if 'quantumleapUrl' in queries:
            params['quantumleapUrl'] = queries['quantumleapUrl']
        else:
            params['quantumleapUrl'] = f'{self.base_url}'
        if 'entityType' in queries:
            params['entityType'] = queries['entityType']
        if 'entityId' in queries:
            params['entityId'] = queries['entityId']
        if 'idPattern' in queries:
            params['idPattern'] = queries['idPattern']
        if 'attributes' in queries:
            params['attributes'] = queries['attributes']
        if 'observedAttributes' in queries:
            params['obervedAttributes'] = queries['observedAttributes']
        if 'notifiedAttributes' in queries:
            params['notifiedAttributes'] = queries['notifiedAttributes']
        if 'throttlinf' in queries:
            params['throttling'] = queries['throttling']
        if 'timeIndexAttribute' in queries:
            params['timeIndexAttribute'] = queries['timeIndexAttribute']
        return params

    def get_version(self):
        url = f'http://{self.host}:{self.port}/version'
        response = self._do_request(method='GET', url=url)
        return response

    def post_config(self, type=None, replicas=None):
        url = f'{self.base_url}/config'
        params = {}
        if type:
            params['type'] = type
        if replicas:
            params['replicas'] = replicas
        response = self._do_request(method='POST', url=url,
                                    queries=params)
        return response

    def get_health(self):
        url = f'http://{self.host}:{self.port}/health'
        response = self._do_request(method='GET', url=url)
        return response

    def post_notify(self, body):
        url = f'{self.base_url}/notify'
        responce = self._do_request(method="POST", url=url, body=body,
                                    headers={"Content-Type":
                                             "application/json"})
        return responce

    def post_subscription(self, **kwargs):
        url = f'{self.base_url}/subscribe'
        params = self.wrap_subscribe_params(queries=kwargs)
        response = self._do_request(method='POST', url=url,
                                    queries=params)
        return response

    def delete_entity_id(self, entity_id: str, **kwargs):
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def delete_entity_type(self, entity_type: str, **kwargs):
        url = f'{self.base_url}/types/{entity_type}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def get_entity_attribute(self, entity_id: str, attr_name: str, **kwargs):
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_attribute_value(self, entity_id: str, attr_name: str,
                                   **kwargs):
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity(self, entity_id: str, **kwargs):
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_value(self, entity_id: str, **kwargs):
        url = f'{self.base_url}/entities/{entity_id}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute(self, entity_type: str, attr_name: str, **kwargs):
        url = f'{self.base_url}/types/{entity_type}/attrs/{attr_name}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute_value(self, **kwargs):
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type(self, entity_type: str, **kwargs):
        url = f'{self.base_url}/types/{entity_type}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_value(self, entity_type: str, **kwargs):
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute(self, attr_name: str, **kwargs):
        url = '{self.base_url}/attrs/{attr_name}'
        params = self.wrap_params(queries=keargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute_value(self, attr_name: str, **kwargs):
        url = '{self.base_url}/attrs/{attr_name}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs(self, **kwargs):
        url = '{self.base_url}/attrs'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs_value(self, **kwargs):
        url = '{self.base_url}/attrs/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response
