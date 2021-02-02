import requests


class QuantumLeapClientException(Exception):

    def __init__(self, status, message, *args, *kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class Client(object):
    def __init__(self, host='localhost', port=1026, service=None, service_path=None):
        pass

    def _do_request(self):
        pass
