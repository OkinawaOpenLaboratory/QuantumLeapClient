from quantumleapclient.client import Client, QuantumLeapClientException

import unittest
from unittest import mock

def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'url_valid':
        return MockResponse({"key1": "value1"}, 200)

    return MockResponse({}, 404)

class TestGetResp(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_resp_ok(self, mock_get):
        client = Client()
        json_data = client._do_request(url='url_valid')
        self.assertEqual(json_data, {"key1": "value1"})

if __name__ == '__main__':
    unittest.main()
