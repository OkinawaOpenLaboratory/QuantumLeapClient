import logging
import json
import requests

from datetime import datetime

logger = logging.getLogger(__name__)
fmt = "%(asctime)s|%(levelname)s |%(name)s:%(funcName)s:%(lineno)s-%(message)s"
logging.basicConfig(level=logging.DEBUG, format=fmt)


class QuantumLeapClientException(Exception):
    logger.debug("init QuantumLeapClientexception")

    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message
        logger.error(f'status:{status}|{message}')


class Client(object):
    logger.debug("init Client")
    version = "v2"

    def __init__(self, host='localhost', port=8668,
                 service=None, service_path=None):
        self.host = host
        self.port = port
        self.base_url = f'http://{host}:{port}/{self.version}'
        self.service = service
        self.service_path = service_path
        logger.info(f'base_url = {self.base_url}')
        pass

    def _do_request(self, method=None, url=None,
                    queries=None, body=None, headers=None):
        logger.info("start _do_request function")
        logger.info(f'url={url}')
        if method == 'POST':
            try:
                response = requests.post(url, params=queries,
                                         data=json.dumps(body),
                                         headers=headers)
                response.raise_for_status()
                logger.info(
                    f'status:{response.status_code}|message:success')
            except requests.exceptions.RequestException as e:
                status = response.status_code
                if status == 400:
                    message = "Bad request."
                elif status == 412:
                    message = "You specified an unreachable Orion"\
                              "url for QuantumLeap."
                elif status == 500:
                    message = "Internal server error."
                elif status == 501:
                    message = "Not implemented!"
                raise QuantumLeapClientException(
                    status=status, message=message)
        elif method == 'DELETE':
            try:
                response = requests.delete(url, params=queries)
                response.raise_for_status()
                logger.info(
                    f'status:{response.status_code}|'
                    'message:Records successfully deleted.')
            except requests.exceptions.RequestException as e:
                status = response.status_code
                if status == 404:
                    message = "Not Found"
                else:
                    message = "Error"
                raise QuantumLeapClientException(
                    status=status, message=message)
        else:
            try:
                res = requests.get(url, params=queries)
                res.raise_for_status()
                logger.info(
                    f'status:{res.status_code}|message:OK')
                response = res.json()
                return response
            except requests.exceptions.RequestException as e:
                status = res.status_code
                if status == 404:
                    message = "Not Found"
                elif status == 501:
                    message = "Not implemented"
                raise QuantumLeapClientException(
                    status=status, message=message)

    def wrap_params(self, queries):
        logger.info("start wrap parameters")
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
        logger.info(f'params={params}')
        return params

    def wrap_subscribe_params(self, queries):
        logger.info("start wrap parameters for subscription")
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
        logger.info(f'params={params}')
        return params

    def get_version(self):
        logger.info("start get_version function")
        url = f'http://{self.host}:{self.port}/version'
        response = self._do_request(method='GET', url=url)
        logger.info("end get_version function")
        return response

    def post_config(self, type=None, replicas=None):
        logger.info("start post_config function")
        url = f'{self.base_url}/config'
        params = {}
        if type:
            params['type'] = type
        if replicas:
            params['replicas'] = replicas
        logger.info(f'params={params}')
        response = self._do_request(method='POST', url=url,
                                    queries=params)
        return response

    def get_health(self):
        logger.info("start get_gealth function")
        url = f'http://{self.host}:{self.port}/health'
        response = self._do_request(method='GET', url=url)
        return response

    def post_notify(self, body):
        logger.info("start post_notify function")
        url = f'{self.base_url}/notify'
        responce = self._do_request(method="POST", url=url, body=body,
                                    headers={"Content-Type":
                                             "application/json"})
        return responce

    def post_subscription(self, **kwargs):
        logger.info("start post_subscription")
        url = f'{self.base_url}/subscribe'
        params = self.wrap_subscribe_params(queries=kwargs)
        response = self._do_request(method='POST', url=url,
                                    queries=params)
        return response

    def delete_entity_id(self, entity_id: str, **kwargs):
        logger.info("start delete_entity_id function")
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def delete_entity_type(self, entity_type: str, **kwargs):
        logger.info("start delete_entity_type function")
        url = f'{self.base_url}/types/{entity_type}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def get_entity_attribute(self, entity_id: str, attr_name: str, **kwargs):
        logger.info("start get_entity_attribute function")
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_attribute_value(self, entity_id: str, attr_name: str,
                                   **kwargs):
        logger.info("start get_entity_attribute_value function")
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity(self, entity_id: str, **kwargs):
        logger.info("start get_entity function")
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_value(self, entity_id: str, **kwargs):
        logger.info("start get_entity_value function")
        url = f'{self.base_url}/entities/{entity_id}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute(self, entity_type: str, attr_name: str, **kwargs):
        logger.info("start get_type_attribute function")
        url = f'{self.base_url}/types/{entity_type}/attrs/{attr_name}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute_value(self, **kwargs):
        logger.info("start get_type_attribute_value function")
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type(self, entity_type: str, **kwargs):
        logger.info("start get_type functioin")
        url = f'{self.base_url}/types/{entity_type}'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_value(self, entity_type: str, **kwargs):
        logger.info("start get_type_value function")
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute(self, attr_name: str, **kwargs):
        logger.info("start get_attribute function")
        url = '{self.base_url}/attrs/{attr_name}'
        params = self.wrap_params(queries=keargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute_value(self, attr_name: str, **kwargs):
        logger.info("start get_attribute_value")
        url = '{self.base_url}/attrs/{attr_name}/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs(self, **kwargs):
        logger.info("start get_attrs function")
        url = '{self.base_url}/attrs'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs_value(self, **kwargs):
        logger.info("start get_attrs function")
        url = '{self.base_url}/attrs/value'
        params = self.wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response
