from logging import ERROR
from unittest import TestCase
from unittest.mock import patch, Mock

import mock

from quantumleapclient.client import Client, QuantumLeapClientException


class TestClient(TestCase):

    def setUp(self):
        self.client = Client(log_level=ERROR)

    def _mock_response(self, status_code=200, content="", json_data=None,
                       raise_for_status=None):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status = mock.Mock()
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
        mock_resp.status_code = status_code
        mock_resp.content = content
        if json_data:
            mock_resp.json = mock.Mock(return_value=json_data)
        return mock_resp

    @mock.patch('requests.get')
    def test_get_version(self, mock):
        expected = {"version": "0.7.5"}
        mock.return_value = self._mock_response(json_data=expected)
        result = self.client.get_version()
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_health(self, mock):
        expected = {'status': 'pass'}
        mock.return_value = self._mock_response(json_data=expected)
        result = self.client.get_health()
        self.assertEqual(expected, result)

    @mock.patch('requests.post')
    def test_post_notify(self, mock):
        mock.return_value = self._mock_response(
                                status_code=201,
                                content="Successfully created record.")
        body = {}
        body["subscription_id"] = "5947d174793fe6f7eb5e3961" # Rondom value
        entity = {}
        entity["id"] = "Room1"
        entity["type"] = "Room"
        entity["temperature"] = {'type': 'Float', 'value': 23.0}
        body["data"] = [entity]
        raised = False
        try:
            result = self.client.post_notify(body)
        except:
            raised = True
        self.assertFalse(raised, 'Exception raised')
