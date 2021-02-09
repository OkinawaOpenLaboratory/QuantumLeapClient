import requests
import json

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

    def _do_request(self, method=None, url=None, queries=None):
        if method == 'POST':
            response = requests.post(url)
        elif method == 'DELETE':
            response = requests.delete(url, params=queries)
            if response.status_code == 204:
                return {"status": "delete historical records."}
            else:
                return {"status": "Error"}
        else:
            response = requests.get(url, params=queries)
        return response.json()

    def delete_entity(self, entity_id: str, type=None,
                      fromDate=None, toDate=None):
        url = f'{self.base_url}/entities/{entity_id}'
        params = {}
        if type:
            parames['type'] = type
        else:
            pass
        if fromDate:
            self.fromDate = fromDate
            if isinstance(self.fromDate, 'datetime'):
                self.fromDate = fromdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['fromDate'] = self.fromDate
            else:
                params['fromDate'] = fromDate
        else:
            pass
        if toDate:
            self.toDate = toDate
            if isinstance(self.toDate, 'datetime'):
                self.toDate = toDate.strftime('%Y-%m-%DT%H:%M:%SZ')
                params['toDate'] = self.toDate
            else:
                params['toDate'] = toDate
        else:
            pass
        response = self._do_request(method='DELETE', url=url)
        return response

    def get_entity_attribute(self, entity_id: str, attr_name: str, type=None,
                             aggrMethod=None, aggrPeriod=None, options=None,
                             fromDate=None, toDate=None, lastN=None,
                             limit=None, offset=None, georel=None,
                             geometory=None, coords=None):
        url = f'{self.base_url}/entities/{entity_id}/attrs/{attr_name}'
        params = {}
        if type:
            params['type'] = type
        else:
            pass
        if aggrMethod:
            params['aggrMethod'] = aggrMethod
        else:
            pass
        if aggrPeriod:
            params['aggrPeriod'] = aggrPeriod
        else:
            pass
        if options:
            params['options'] = options
        else:
            pass
        if fromDate:
            self.fromDate = fromDate
            if isinstance(fromDate, datetime):
                self.fromDate = fromdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['fromDate'] = self.fromDate
            else:
                params['fromDate'] = fromDate
        else:
            pass
        if toDate:
            self.toDate = toDate
            if isinstance(toDate, datetime):
                self.toDate = toDate.strftime('%Y-%m-%dT%H:%M:%SZ')
                params['toDate'] = self.toDate
            else:
                params['toDate'] = toDate
        else:
            pass
        if lastN:
            params['lastN'] = lastN
        else:
            pass
        if limit:
            params['limit'] = limit
        else:
            pass
        if offset:
            params['offset'] = offset
        else:
            pass
        if georel:
            params['georel'] = georel
        else:
            pass
        if geometory:
            params['geometory'] = geometory
        else:
            pass
        if coords:
            params['coords'] = coords
        else:
            pass
        response = self._do_request(method='GET', url=url, queries=params)
        return response
