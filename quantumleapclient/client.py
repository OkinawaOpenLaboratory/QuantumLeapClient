import requests
import json

class QuantumLeapClientException(Exception):

    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class Client(object):
    version = "v2"
    def __init__(self, host='localhost', port=8668, service=None, service_path=None):
        self.base_url = 'http://' + host + ':' + str(port) + '/' + self.version
        self.service = service
        self.service_path = service_path
        pass

    def _do_request(self, method=None, url=None, queries=None):
        if method == 'POST':
            response = requests.post(url)
        elif method == 'DELETE':
            response = requests.delete(url)
            if response.status_code == 204:
                return {status: "delete historical records."}
            else:
                return {status: "Error"}
        else:
            response = requests.get(url)
        return response.json()

    def delete_entity(self, enity_id=None):
        url = (self.base.url
               + '/entities/' + entity_id)
        response = self._do_request(method='POST', url=url)
        return response

    def get_entity_attribute(self, entity_id=None, attr_name=None, type=None,
                             aggrMethod=None, aggrPeriod=None, options=None,
                             fromDate=None, toDate=None, lastN=None):
        url = (self.base_url
               + '/entities/' + entity_id
               + '/attrs/' + attr_name)
        response = self._do_request(method='GET', url=url)
        return response
