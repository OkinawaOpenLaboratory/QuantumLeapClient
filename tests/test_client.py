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
        expected = {"version": "0.7.6"}
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

    @mock.patch('requests.post')
    def test_post_subscription(self, mock):
        mock.return_value = self._mock_response(
                                status_code=201,
                                content="Successfully created subscription.")
        raised = False
        try:
            result = self.client.post_subscription(orionUrl='http://fiware-orion:1026/v2',
                                                quantumleapUrl='http://localhost:8668/v2')
        except:
            raised = True
        self.assertFalse(raised, 'Exception raised')

    @mock.patch('requests.delete')
    def test_delete_entity_id(self, mock):
        mock.return_value = self._mock_response(
                                status_code=204,
                                content="Successfully delete record.")
        raised = False
        try:
            result = self.client.delete_entity_id(entity_id='Room1')
        except:
            raised = True
        self.assertFalse(raised, 'Exception raised')

    @mock.patch('requests.delete')
    def test_delete_entity_type(self, mock):
        mock.return_value = self._mock_response(
                                status_code=204,
                                content="Successfully delete record")
        raised = False
        try:
            result = self.client.delete_entity_type(entity_type='Room')
        except:
            raised = True
        self.assertFalse(raised, 'Exception raised')

    @mock.patch('requests.get')
    def test_get_entity_attribute(self, mock):
        expected = {'attrName': 'temperature',
                    'entityId': 'Room1',
                    'index': ['2021-03-02T04:58:27.203+00:00'],
                    'values': ['23.0']}
        mock.return_value = self._mock_response(json_data=expected)
        result = self.client.get_entity_attribute(entity_id='Room1',
                                                  attr_name='temperature',
                                                  limit=1)
        self.assertEqual(expected, result)
    
    @mock.patch('requests.get')
    def test_get_entity_attribute_value(self, mock):
        expected = {'index': ['2021-03-02T04:58:27.203+00:00'],
                    'values': ['23.0']}
        mock.return_value = self._mock_response(json_data=expected)
        result = self.client.get_entity_attribute_value(entity_id='Room1',
                                                        attr_name='temperature',
                                                        limit=1)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_entity(self, mock):
        expected = {'attributes': [
                       {'attrName': 'pressure', 'values': ['720']},
                       {'attrName': 'temperature', 'values': ['23.0']}],
                    'entityId': 'Room1',
                    'index': ['2021-03-02T04:58:27.203+00:00']}
        mock.return_value = self._mock_response(json_data=expected)
        result = self.client.get_entity(entity_id='Room1', limit=1)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_entity_value(self, mock):
        expected = {'attributes': [{'attrName': 'pressure','values': ['720', '720', '720']},
                                   {'attrName': 'temperature', 'values': ['23.0', '23.0', '23.0']}],
                    'index': ['2021-03-02T04:58:27.203+00:00', '2021-03-02T04:58:27.203+00:00',
                              '2021-03-02T04:58:27.203+00:00']}
        response = {'attributes': [{'attrName': 'pressure','values': ['720']},
                                   {'attrName': 'temperature', 'values': ['23.0']}],
                    'index': ['2021-03-02T04:58:27.203+00:00']}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_entity_value(entity_id='Room1', limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_type_attribute(self, mock):
        expected = {'attrName': 'temperature',
                   'entities': [
                       {'entityId': 'Room2',
                        'index': ['2021-01-29T01:44:57.529+00:00',
                                  '2021-01-29T01:44:57.529+00:00',
                                  '2021-01-29T01:44:57.529+00:00'],
                        'values': ['21', '21', '21']}],
                   'entityType': 'Room'}
        response = {'attrName': 'temperature',
                   'entities': [
                       {'entityId': 'Room2',
                        'index': ['2021-01-29T01:44:57.529+00:00'],
                        'values': ['21']}],
                   'entityType': 'Room'}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_type_attribute(entity_type='Room', attr_name='temperature',
                                                 limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_type_attribute_value(self, mock):
        expected = {'values': [{'entityId': 'Room2',
                                'index': ['2021-01-29T01:44:57.529+00:00',
                                          '2021-01-29T01:44:57.529+00:00',
                                          '2021-01-29T01:44:57.529+00:00'],
                                'values': ['21', '21', '21']}]}
        response = {'values': [{'entityId': 'Room2',
                                'index': ['2021-01-29T01:44:57.529+00:00'],
                                'values': ['21']}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_type_attribute_value(entity_type='Room', attr_name='temperature',
                                                      limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_type(self, mock):
        expected = {'entities': [{'attributes': [{'attrName': 'pressure',
                                                  'values': ['711', '711', '711']},
                                                 {'attrName': 'temperature',
                                                  'values': ['21', '21', '21']}],
                                  'entityId': 'Room2',
                                  'index': ['2021-01-29T01:44:57.529+00:00',
                                            '2021-01-29T01:44:57.529+00:00',
                                            '2021-01-29T01:44:57.529+00:00']}],
                    'entityType': 'Room'}
        response = {'entities': [{'attributes': [{'attrName': 'pressure',
                                                  'values': ['711']},
                                                 {'attrName': 'temperature',
                                                  'values': ['21']}],
                                  'entityId': 'Room2',
                                  'index': ['2021-01-29T01:44:57.529+00:00']}],
                    'entityType': 'Room'}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_type(entity_type='Room', limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_type_value(self, mock):
        expected = {'values': [{'attributes': [{'attrName': 'pressure',
                                                'values': ['711', '711', '711']},
                                               {'attrName': 'temperature',
                                                'values': ['21', '21', '21']}],
                                 'entityId': 'Room2',
                                 'index': ['2021-01-29T01:44:57.529+00:00',
                                           '2021-01-29T01:44:57.529+00:00',
                                           '2021-01-29T01:44:57.529+00:00']}]}
        response = {'values': [{'attributes': [{'attrName': 'pressure',
                                                'values': ['711']},
                                               {'attrName': 'temperature',
                                                'values': ['21']}],
                                 'entityId': 'Room2',
                                 'index': ['2021-01-29T01:44:57.529+00:00']}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_type_value(entity_type='Room', limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_attribute(self, mock):
        expected = {'attrName': 'temperature',
                    'types': [{'entities': [{'entityId': 'Room2',
                                             'index': ['2021-01-29T01:44:57.529+00:00',
                                                       '2021-01-29T01:44:57.529+00:00',
                                                       '2021-01-29T01:44:57.529+00:00'],
                                             'values': ['21', '21', '21']}],
                    'entityType': 'Room'}]}
        response = {'attrName': 'temperature',
                    'types': [{'entities': [{'entityId': 'Room2',
                                             'index': ['2021-01-29T01:44:57.529+00:00'],
                                             'values': ['21']}],
                    'entityType': 'Room'}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_attribute(attr_name='temperature', limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_attribute_value(self, mock):
        expected = {'values': [{'entities': [{'entityId': 'Room2',
                                             'index': ['2021-01-29T01:44:57.529+00:00',
                                                       '2021-01-29T01:44:57.529+00:00',
                                                       '2021-01-29T01:44:57.529+00:00'],
                                             'values': ['21', '21', '21']}],
                    'entityType': 'Room'}]}
        response = {'values': [{'entities': [{'entityId': 'Room2',
                                             'index': ['2021-01-29T01:44:57.529+00:00'],
                                             'values': ['21']}],
                    'entityType': 'Room'}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_attribute_value(attr_name='temperature', limit=20000)
        self.assertEqual(expected, result)


    @mock.patch('requests.get')
    def test_get_attrs(self, mock):
        expected = {'attrs': [{'attrName': 'pressure',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['711', '711', '711']}],
                                                        'entityType': 'Room'}]},
                              {'attrName': 'temperature',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['21', '21', '21']}],
                                                        'entityType': 'Room'}]}]}
        response = {'attrs': [{'attrName': 'pressure',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['711']}],
                                                        'entityType': 'Room'}]},
                              {'attrName': 'temperature',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['21']}],
                                                        'entityType': 'Room'}]}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_attrs(limit=20000)
        self.assertEqual(expected, result)

    @mock.patch('requests.get')
    def test_get_attrs_value(self, mock):
        expected = {'values': [{'attrName': 'pressure',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['711', '711', '711']}],
                                                        'entityType': 'Room'}]},
                              {'attrName': 'temperature',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00',
                                                                  '2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['21', '21', '21']}],
                                                        'entityType': 'Room'}]}]}
        response = {'values': [{'attrName': 'pressure',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['711']}],
                                                        'entityType': 'Room'}]},
                              {'attrName': 'temperature',
                               'types': [{'entities': [{'entityId': 'Room2',
                                                        'index': ['2021-01-29T01:44:57.529+00:00'],
                                                        'values': ['21']}],
                                                        'entityType': 'Room'}]}]}
        mock.return_value = self._mock_response(json_data=response)
        result = self.client.get_attrs_value(limit=20000)
        self.assertEqual(expected, result)

