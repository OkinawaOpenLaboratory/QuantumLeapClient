import json
import requests

from datetime import datetime


class QuantumLeapClientException(Exception):
    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class Client(object):
    version = "v2"

    def __init__(self, host='localhost', port=8668,
                 service=None, service_path=None):
        self.base_url = f'http://{host}:{port}/{self.version}'
        self.service = service
        self.service_path = service_path
        pass

    def _do_request(self, method=None, url=None, queries=None, body=None):
        if method == 'POST':
            response = requests.post(url,
                                     data=json.dumps(body),
                                     headers={"Content-Type":
                                              "application/json"})
            if response.status_code == 200:
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

    def wrap_params(self, type=None, aggrMethod=None, aggrPeriod=None,
                    options=None, fromDate=None, toDate=None,
                    lastN=None, limit=None, offset=None,
                    georel=None, geometory=None, coords=None,
                    id=None, aggrScope=None):
        params = {}
        if type:
            params['type'] = type
        if aggrMethod:
            params['aggrMethod'] = aggrMethod
        if aggrPeriod:
            params['aggrPeriod'] = aggrPeriod
        if options:
            params['options'] = options
        if fromDate:
            self.fromDate = fromDate
            if isinstance(fromDate, datetime):
                self.fromDate = fromdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['fromDate'] = self.fromDate
            else:
                params['fromDate'] = fromDate
        if toDate:
            self.toDate = toDate
            if isinstance(toDate, datetime):
                self.toDate = toDate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['toDate'] = self.toDate
            else:
                params['toDate'] = toDate
        if lastN:
            params['lastN'] = lastN
        if limit:
            params['limit'] = limit
        if offset:
            params['offset'] = offset
        if georel:
            params['georel'] = georel
        if geometory:
            params['geometory'] = geometory
        if coords:
            params['coords'] = coords
        if id:
            params['id'] = id
        if aggrScope:
            params['aggrScope'] = aggrScope
        return params

    def post_notify(self, body):
        url = f'{self.base_url}/notify'
        responce = self._do_request(method="POST", url=url, body=body)
        return responce

    def delete_entity(self, entity_id: str, type=None,
                      fromDate=None, toDate=None):
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(type=type, fromDate=fromDate,
                                  toDate=toDate)
        response = self._do_request(method='DELETE', url=url, queries=params)
        return response

    def get_entity_attribute(self, entity_id: str, attr_name: str, type=None,
                             aggrMethod=None, aggrPeriod=None, options=None,
                             fromDate=None, toDate=None, lastN=None,
                             limit=None, offset=None, georel=None,
                             geometory=None, coords=None):
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}'
        params = self.wrap_params(type=type, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, options=options,
                                  fromDate=fromDate, toDate=toDate,
                                  lastN=lastN, limit=limit,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_attribute_value(self, entity_id: str, attr_name: str,
                                   type=None, aggrMethod=None,
                                   aggrPeriod=None, options=None,
                                   fromDate=None, toDate=None, lastN=None,
                                   limit=None, offset=None, georel=None,
                                   geometory=None, coords=None):
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}/value'
        params = self.wrap_params(type=type, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, options=options,
                                  fromDate=fromDate, toDate=toDate,
                                  lastN=lastN, limit=limit,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity(self, entity_id: str, type=None,
                   aggrMethod=None, aggrPeriod=None, options=None,
                   fromDate=None, toDate=None, lastN=None,
                   limit=None, offset=None, georel=None,
                   geometory=None, coords=None):
        url = f'{self.base_url}/entities/{entity_id}'
        params = self.wrap_params(type=type, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod,  options=options,
                                  fromDate=fromDate, toDate=toDate,
                                  lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_entity_value(self, entity_id: str, type=None,
                         aggrMethod=None, aggrPeriod=None, options=None,
                         fromDate=None, toDate=None, lastN=None,
                         limit=None, offset=None, georel=None,
                         geometory=None, coords=None):
        url = f'{self.base_url}/entities/{entity_id}/value'
        params = self.wrap_params(type=type, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod,  options=options,
                                  fromDate=fromDate, toDate=toDate,
                                  lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute(self, entity_type: str, attr_name: str,
                           id=None, aggrMethod=None,
                           aggrPeriod=None, aggrScope=None,
                           options=None, fromDate=None, toDate=None,
                           lastN=None, limit=None, offset=None,
                           georel=None, geometory=None, coords=None):
        url = f'{self.base_url}/types/{entity_type}/attrs/{attr_name}'
        params = self.wrap_params(id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_attribute_value(self, entity_type: str, attr_name: str,
                                 id=None, aggrMethod=None,
                                 aggrPeriod=None, aggrScope=None,
                                 options=None, fromDate=None,
                                 toDate=None, lastN=None, limit=None,
                                 offset=None, georel=None,
                                 geometory=None, coords=None):
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type(self, entity_type: str, id=None, affrMethod=None,
                 aggrPeriod=None, aggrScope=None, options=None,
                 fromDate=None, toDate=None, lastN=None, limit=None,
                 offset=None, georel=None, geometory=None,
                 coords=None):
        url = f'{self.base_url}/types/{entity_type}'
        params = self.wrap_params(id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_type_value(self, entity_type: str, id=None, aggrMethod=None,
                       aggrPeriod=None, aggrScope=None, options=None,
                       fromDate=None, toDate=None, lastN=None, limit=None,
                       offset=None):
        url = f'{self.base_url}/types/{entity_type}/value'
        params = self.wrap_params(id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute(self, attr_name: str, type=None, id=None,
                      aggrMethod=None, aggrPeriod=None, aggrScope=None,
                      options=None, fromDate=None, toDate=None,
                      lastN=None, limit=None, offset=None):
        url = '{self.base_url}/attrs/{attr_name}'
        params = self.wrap_params(type=type, id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attribute_value(self, attr_name: str, type=None, id=None,
                            aggrMethod=None, aggrPeriod=None, aggrScope=None,
                            options=None, fromDate=None, toDate=None,
                            lastN=None, limit=None, offset=None, georel=georel,
                            geometory=geometory, coords=coords):
        url = '{self.base_url}/attrs/{attr_name}/value'
        params = self.wrap_params(type=type, id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georrel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs(self, type=None, id=None, aggrMethod=None, aggrPeriod=None,
                  aggrScope=None, options=None, fromDate=None, toDate=None,
                  lastN=None, limit=None, offset=None, georel=georel,
                  geometory=geometory, coords=coords):
        url = '{self.base_url}/attrs'
        params = self.wrap_params(type=type, id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georrel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response

    def get_attrs_value(self, type=None, id=None, aggrMethod=None,
                        aggrPeriod=None, aggrScope=None, options=None,
                        fromDate=None, toDate=None, lastN=None,
                        limit=None, offset=None, georel=georel,
                        geometory=geometory, coords=coords):
        url = '{self.base_url}/attrs/value'
        params = self.wrap_params(type=type, id=id, aggrMethod=aggrMethod,
                                  aggrPeriod=aggrPeriod, aggrScope=aggrScope,
                                  options=options, fromDate=fromDate,
                                  toDate=toDate, lastN=lastN, limit=limit,
                                  offset=offset, georrel=georel,
                                  geometory=geometory, coords=coords)
        response = self._do_request(method='GET', url=url, queries=params)
        return response
