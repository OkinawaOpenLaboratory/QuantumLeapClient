import logging
import json
import requests

from datetime import datetime

logger = logging.getLogger(__name__)
fmt = "%(asctime)s|%(levelname)s |%(name)s:%(funcName)s:%(lineno)s-%(message)s"


class QuantumLeapClientException(Exception):
    logger.debug("init QuantumLeapClientexception")

    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message
        logger.error(f'status:{status}|{message}')


class Client(object):

    def __init__(self, host='localhost', port=8668, service=None,
                 service_path=None, log_level=logging.INFO):
        logger.debug("init Client")
        self.host = host
        self.port = port
        self.base_url = f'http://{host}:{port}/v2'
        self.service = service
        self.service_path = service_path
        logging.basicConfig(level=log_level, format=fmt)
        logger.info(f'base_url = {self.base_url}')

    def __append_index(self, index1, index2):
        index_list = []
        index_list.extend(index1)
        index_list.extend(index2)
        return index_list

    def __append_values(self, values1, values2):
        values_list = []
        values_list.extend(values1)
        values_list.extend(values2)
        return values_list

    def __append_attributes(self, attributes1, attributes2):
        attributes_list = []
        for attribute1 in attributes1:
            attribute = {}
            for attribute2 in attributes2:
                if attribute1["attrName"] == attribute2["attrName"]:
                    attribute["attrName"] = attribute1["attrName"]
                    attribute["values"] = self.__append_values(
                        attribute1["values"], attribute2["values"])
                    break
            if "attrName" in attribute:
                attributes_list.append(attribute)
            else:
                attributes_list.append(attribute1)
        for attribute2 in attributes2:
            attribute = {}
            for attribute1 in attributes1:
                if attribute2["attrName"] == attribute1["attrName"]:
                    attribute["attrName"] = attribute2["attrName"]
                    break
            if "attrName" not in attribute:
                attributes_list.append(attribute2)
        return sorted(attributes_list, key=lambda x: x["attrName"])

    def __append_entities(self, entities1, entities2):
        entities_list = []
        if "values" in entities2:
            for entity1 in entities1:
                entity = {}
                for entity2 in entities2:
                    if entity1["entityId"] == entity2["entityId"]:
                        entity["entityId"] = entity1["entityId"]
                        entity["index"] = self.__append_index(
                            entity1["index"], entity2["index"])
                        entity["values"] = self.__append_values(
                            entity1["values"], entity2["values"])
                        break
                if "entityId" in entity:
                    entities_list.append(entity)
                else:
                    entities_list.append(entity1)
            for entity2 in entities2:
                entity = {}
                for entity1 in entities1:
                    if entity2["entityId"] == entity1["entityId"]:
                        entity["entityId"] = entity2["entityId"]
                        break
                if "entityId" not in entity:
                    entities_list.append(entity2)
        else:
            for entity1 in entities1:
                entity = {}
                for entity2 in entities2:
                    if entity1["entityId"] == entity2["entityId"]:
                        entity["entitytId"] = entity1["entityId"]
                        entity["index"] = self.__append_index(
                            entity1["index"], entity2["index"])
                        entity["attributes"] = self.__append_attributes(
                            entity1["attributes"],
                            entity2["attributes"])
                        break
                if "entityId" in entity:
                    entities_list.append(entity)
                else:
                    entities_list.append(entity1)
            for entity2 in entities2:
                entitiy = {}
                for entity1 in entities1:
                    if entity2["entityId"] == entity1["entityId"]:
                        entity["entityId"] = entity2["entityId"]
                        break
                if "entityId" not in entity:
                    entities_list.append(entity2)
        return sorted(entities_list, key=lambda x: x["entityId"])

    def __append_types(self, types1, types2):
        types_list = []
        for type1 in types1:
            entity_type = {}
            for type2 in types2:
                if type1["entityType"] == type2["entityType"]:
                    entity_type["entityType"] = type1["entityType"]
                    entities1 = type1["entities"]
                    entities2 = type2["entities"]
                    entity["entities"] = self.__append_entities(
                        entities1, entities2)
                    break
            if "entityType" in entity_type:
                types_list.append(entity_type)
            else:
                types_list.append(type1)
        for type2 in types2:
            entity_type = {}
            for type1 in types1:
                if type2["entityType"] == type1["entityType"]:
                    entity_type["entityType"] = type2["entityType"]
                    break
            if "entityType" not in entity_type:
                types_list.append(types_list)
        return sorted(types_list, key=lambda x: x["entityType"])

    def __append_attrs(self, attrs1, attrs2):
        attrs_list = []
        for attr1 in attrs1:
            attr = {}
            for attr2 in attrs2:
                if attr1["attrName"] == attr2["attrName"]:
                    attr["attrName"] = attr1["attrName"]
                    types1 = attr1["types"]
                    types2 = attr2["types"]
                    attr["types"] = self.__append_types(
                        types1, types2)
                    break
            if "attrName" in attr:
                attr_list.append(attr)
            else:
                attr_list.append(attr1)
        for attr2 in attrs2:
            attr = {}
            for attr1 in attrs1:
                if attr2["attrName"] == attr1["attrName"]:
                    attr["attrName"] = attr2["attrName"]
                    break
            if "attrName" not in attr:
                attr_list.append(attr2)
        return sorted(attrs_list, key=lambda x: x["attrName"])

    def __append_response(self, response1, response2):
        __append_response = {}
        if "attrName" in response1:
            __append_response["attrName"] = response1["attrName"]
        if "entityId" in response1:
            __append_response["entityId"] = response1["entityId"]
        if "index" in response1:
            if "index" in response2:
                __append_response["index"] = self.__append_index(
                    response1["index"], response2["index"])
            else:
                __append_response["index"] = response1["index"]
        if "values" in response1:
            if "values" in response2:
                values_check = response1["values"]
                if "entityId" in values_check[0]:
                    __append_response["values"] = self.__append_entities(
                        response1["values"], response2["values"])
                elif "entities" in values_check[0]:
                    __append_response["values"] = self.__append_types(
                        response1["values"], response2["values"])
                elif "types" in values_check[0]:
                    __append_response["values"] = self.__append_attrs(
                        response1["values"], response2["values"])
                else:
                    __append_response["values"] = self.__append_values(
                        response1["values"], response2["values"])
            else:
                __append_response["values"] = response1["values"]
        if "attributes" in response1:
            if "attributes" in response2:
                __append_response["attributes"] = self.__append_attributes(
                    response1["attributes"], response2["attributes"])
            else:
                __append_response["attributes"] = response1["attributes"]
        if "entities" in response1:
            if "entities" in response2:
                __append_response["entities"] = self.__append_entities(
                    response1["entities"], response2["entities"])
            else:
                __append_response["entities"] = response1["entities"]
        if "entityType" in response1:
            __append_response["entityType"] = response1["entityType"]
        if "types" in response1:
            if "types" in response2:
                __append_response["types"] = self.__append_types(
                    response1["types"], response2["types"])
            else:
                __append_response["types"] = response1["types"]
        if "attrs" in response1:
            if "attrs" in response2:
                __append_response["attrs"] = self.__append_attrs(
                    response1["attrs"], response2["attrs"])
            else:
                __append_response["attrs"] = response1["attrs"]
        return __append_response

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
                append_response = {}
                while True:
                    response = requests.get(url, params=queries)
                    response_dict = response.json()
                    if response.status_code == 200:
                        logger.info(
                            f'status:{response.status_code}'
                            'message: Successful response.')
                        append_response = self.__append_response(
                            response_dict, append_response)
                        if "offset" in queries:
                            queries["offset"] += queries["limit"]
                        else:
                            queries["offset"] = queries["limit"]
                    else:
                        logger.info(
                            f'status:{response.status_code}|'
                            'message:End roop process')
                        break
                return append_response
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
                response = request.get(url, params=queries)
                __append_response = resp.json()
                if "offset" in queries:
                    queries["offset"] += queries["limit"]
                else:
                    queries["offset"] = queries["limit"]
                queries["limit"] = 10000
                for i in range(loop_count):
                    response = requests.get(url, params=queries)
                    response_dict = response.json()
                    append_response = self.__append_response(
                        response_dict, append_response)
                    queries["offset"] += queries["limit"]
                return append_response

    def __wrap_params(self, queries):
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

    def __wrap_subscribe_params(self, queries):
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
        if 'throttling' in queries:
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
        params = self.__wrap_subscribe_params(queries=kwargs)
        response = self._do_request(method='POST', url=url,
                                    queries=params)
        return response

    def delete_entity_id(self, entity_id: str, **kwargs):
        logger.info("start delete_entity_id function")
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def delete_entity_type(self, entity_type: str, **kwargs):
        logger.info("start delete_entity_type function")
        url = f'{self.base_url}/types/{entity_type}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def get_entity_attribute(self, entity_id: str, attr_name: str, **kwargs):
        logger.info("start get_entity_attribute function")
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_attribute_value(self, entity_id: str, attr_name: str,
                                   **kwargs):
        logger.info("start get_entity_attribute_value function")
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}/value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity(self, entity_id: str, **kwargs):
        logger.info("start get_entity function")
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_value(self, entity_id: str, **kwargs):
        logger.info("start get_entity_value function")
        url = f'{self.base_url}/entities/{entity_id}/value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute(self, entity_type: str, attr_name: str, **kwargs):
        logger.info("start get_type_attribute function")
        url = f'{self.base_url}/types/{entity_type}/attrs/{attr_name}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute_value(self, entity_type: str,
                                 attr_name: str, **kwargs):
        logger.info("start get_type_attribute_value function")
        url = f'{self.base_url}/types/{entity_type}/attrs/{attr_name}value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type(self, entity_type: str, **kwargs):
        logger.info("start get_type functioin")
        url = f'{self.base_url}/types/{entity_type}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_value(self, entity_type: str, **kwargs):
        logger.info("start get_type_value function")
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute(self, attr_name: str, **kwargs):
        logger.info("start get_attribute function")
        url = f'{self.base_url}/attrs/{attr_name}'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute_value(self, attr_name: str, **kwargs):
        logger.info("start get_attribute_value")
        url = f'{self.base_url}/attrs/{attr_name}/value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs(self, **kwargs):
        logger.info("start get_attrs function")
        url = f'{self.base_url}/attrs'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs_value(self, **kwargs):
        logger.info("start get_attrs function")
        url = f'{self.base_url}/attrs/value'
        params = self.__wrap_params(queries=kwargs)
        response = self._do_request(method='GET', url=url, queries=params)
        return response
