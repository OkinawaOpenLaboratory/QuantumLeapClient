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

    def combine_index(self, res_index, response_index):
        index_list = []
        index_list.extend(response_index)
        index_list.extend(res_index)
        return index_list

    def combine_values(self, res_values, response_values):
        values_list = []
        values_list.extend(response_values)
        values_list.extend(res_values)
        return values_list

    def combine_attributes(self, res_attributes, response_attributes):
        attributes_list = []
        for res_attribute in res_attributes:
            attribute = {}
            for response_attribute in response_attributes:
                if res_attribute["attrName"] == response_attribute["attrName"]:
                    attribute["attrName"] = res_attribute["attrName"]
                    attribute["values"] = self.combine_values(
                        res_attribute["values"], response_attribute["values"])
                    break
            if "attrName" in attribute:
                attributes_list.append(attribute)
            else:
                attributes_list.append(res_attribute)
        for response_attribute in response_attributes:
            attribute = {}
            for res_attribute in res_attributes:
                if response_attribute["attrName"] == res_attribute["attrName"]:
                    attribute["attrName"] = response_attribute["attrName"]
                    break
            if "attrName" not in attribute:
                attributes_list.append(response_attribute)
        return sorted(attributes_list, key=lambda x: x["attrName"])

    def combine_entities(self, res_entities, response_entities):
        entities_list = []
        if "values" in response_entities:
            for res_entity in res_entities:
                entity = {}
                for response_entity in response_entities:
                    if res_entity["entityId"] == response_entity["entityId"]:
                        entity["entityId"] = res_entity["entityId"]
                        entity["index"] = self.combine_index(
                            res_entity["index"], response_entity["index"])
                        entity["values"] = self.combine_values(
                            res_entity["values"], response_entity["values"])
                        break
                if "entityId" in entity:
                    entities_list.append(entity)
                else:
                    entities_list.append(res_entity)
            for response_entity in response_entities:
                entity = {}
                for res_entity in res_entities:
                    if response_entity["entityId"] == res_entity["entityId"]:
                        entity["entityId"] = response_entity["entityId"]
                        break
                if "entityId" not in entity:
                    entities_list.append(response_entity)
        else:
            for res_entity in res_entities:
                entity = {}
                for response_entity in response_entities:
                    if res_entity["entityId"] == response_entity["entityId"]:
                        entity["entitytId"] = res_entity["entityId"]
                        entity["index"] = self.combine_index(
                            res_entity["index"], response_entity["index"])
                        entity["attributes"] = self.combine_attributes(
                            res_entity["attributes"],
                            response_entity["attributes"])
                        break
                if "entityId" in entity:
                    entities_list.append(entity)
                else:
                    entities_list.append(res_entity)
            for response_entity in response_entities:
                entitiy = {}
                for res_entity in res_entities:
                    if response_entity["entityId"] == res_entity["entityId"]:
                        entity["entityId"] = response_entity["entityId"]
                        break
                if "entityId" not in entity:
                    entities_list.append(response_entity)
        return sorted(entities_list, key=lambda x: x["entityId"])

    def combine_types(self, res_types, response_types):
        types_list = []
        for res_type in res_types:
            entity_type = {}
            for response_type in response_types:
                if res_type["entityType"] == response_type["entityType"]:
                    entity_type["entityType"] = res_type["entityType"]
                    res_entities = res_type["entities"]
                    response_entities = response_type["entities"]
                    entity["entities"] = self.combine_entities(
                        res_entities, response_entities)
                    break
            if "entityType" in entity_type:
                types_list.append(entity_type)
            else:
                types_list.append(res_type)
        for response_type in response_types:
            entity_type = {}
            for res_type in res_types:
                if response_type["entityType"] == res_type["entityType"]:
                    entity_type["entityType"] = response_type["entityType"]
                    break
            if "entityType" not in entity_type:
                types_list.append(types_list)
        return sorted(types_list, key=lambda x: x["entityType"])

    def combine_attrs(self, res_attrs, response_attrs):
        attrs_list = []
        for res_attr in res_attrs:
            attr = {}
            for response_attr in response_attrs:
                if res_attr["attrName"] == response_attr["attrName"]:
                    attr["attrName"] = res_attr["attrName"]
                    res_types = res_attr["types"]
                    response_types = response_attr["types"]
                    attr["types"] = self.combine_types(
                        res_types, response_types)
                    break
            if "attrName" in attr:
                attr_list.append(attr)
            else:
                attr_list.append(res_attr)
        for response_attr in response_attrs:
            attr = {}
            for res_attr in res_attrs:
                if response_attr["attrName"] == res_attr["attrName"]:
                    attr["attrName"] = response_attr["attrName"]
                    break
            if "attrName" not in attr:
                attr_list.append(response_attr)
        return sorted(attrs_list, key=lambda x: x["attrName"])

    def combine_response(self, res, response):
        combine_response = {}
        if "attrName" in res:
            combine_response["attrName"] = res["attrName"]
        if "entityId" in res:
            combine_response["entityId"] = res["entityId"]
        if "index" in res:
            if "index" in response:
                combine_response["index"] = self.combine_index(
                    res["index"], response["index"])
            else:
                combine_response["index"] = res["index"]
        if "values" in res:
            if "values" in response:
                values_check = res["values"]
                if "entityId" in values_check[0]:
                    combine_response["values"] = self.combine_entities(
                        res["values"], response["values"])
                elif "entities" in values_check[0]:
                    combine_response["values"] = self.combine_types(
                        res["values"], response["values"])
                elif "types" in values_check[0]:
                    combine_response["values"] = self.combine_attrs(
                        res["values"], response["values"])
                else:
                    combine_response["values"] = self.combine_values(
                        res["values"], response["values"])
            else:
                combine_response["values"] = res["values"]
        if "attributes" in res:
            if "attributes" in response:
                combine_response["attributes"] = self.combine_attributes(
                    res["attributes"], response["attributes"])
            else:
                combine_response["attributes"] = res["attributes"]
        if "entities" in res:
            if "entities" in response:
                combine_response["entities"] = self.combine_entities(
                    res["entities"], response["entities"])
            else:
                combine_response["entities"] = res["entities"]
        if "entityType" in res:
            combine_response["entityType"] = res["entityType"]
        if "types" in res:
            if "types" in response:
                combine_response["types"] = self.combine_types(
                    res["types"], response["types"])
            else:
                combine_response["types"] = res["types"]
        if "attrs" in res:
            if "attrs" in response:
                combine_response["attrs"] = self.combine_attrs(
                    res["attrs"], response["attrs"])
            else:
                combine_response["attrs"] = res["attrs"]
        return combine_response

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
            if queries is None:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    logger.info(
                        f'status:{response.status_code}|'
                        'message:Successful response.')
                    return response.json()
                except requests.exceptions.RequestException as e:
                    status = response.status_code
                    if status == 404:
                        message = "Not Found"
                    else:
                        message = "Error"
                    raise QuantumLeapClientException(
                        status=status, message=message)
            elif "limit" not in queries:
                queries["limit"] = 10000
                response = {}
                while True:
                    resp = requests.get(url, params=queries)
                    res = resp.json()
                    if resp.status_code == 200:
                        logger.info(
                            f'status:{resp.status_code}'
                            'message: Successful response.'
                        response = self.combine_response(
                            res=res, response=response)
                        if "offset" in queries:
                            queries["offset"] += queries["limit"]
                        else:
                            queries["offset"] = queries["limit"]
                    else:
                        logger.info(
                            f'status:{resp.status_code}|'
                            'message:End roop process')
                        break
                return response
            elif queries["limit"] <= 10000:
                try:
                    response = requests.get(url, params=queries)
                    response.raise_for_status()
                    logger.info(
                        f'status:{response.status_code}|'
                        'message:Successful response.')
                    return response.json()
                except requests.exceptions.RequestException as e:
                    status = response.status_code
                    if status == 404:
                        message = "Not Found"
                    else:
                        message = "Error"
                    raise QuantumLeapClientException(
                        status=status, message=message)
            else:
                loop_count = queries["limit"] // 10000
                init_limit = queries["limit"] % 10000
                queries["limit"] = init_limit
                resp = request.get(url. params=queries)
                response = resp.json()
                if "offset" in queries:
                    queries["offset"] += queries["limit"]
                else:
                    queries["offset"] = queries["limit"]
                queries["limit"] = 10000
                for i in range(loop_count):
                    resp = requests.get(url. params=queries)
                    res = resp.json()
                    response = self.combine_response(
                        res=res, response=response)
                    queries["offset"] += queries["limit"]
                return response

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
