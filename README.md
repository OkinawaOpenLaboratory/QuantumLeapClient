# Quantum Leap Client Library for Python

## Install

```bash
pip install -r requirements.txt
```

```bash
python setup.py install
```

## What is it?
 'QuantumLeapClient' is a Python client library  
that helps QuantumLeap API calls.

## Key Features

- Quantum Leap version > 0.7.6 
- [NGSI v2](https://fiware.github.io/specifications/ngsiv2/stable) support
- Calls [QuantumLeap API](https://app.swaggerhub.com/apis/smartsdk/ngsi-tsdb/0.7) in python
- Handle datetime object as a parameter
- Get all historical data in QuantumLeap 
- Allow visualization/debbugging feature.

## Usage

### Initialize QuantumLeapClient

Import and initialize QuantumLeapClient.

```python
from quantumleapclient.client import Client

client = Client(host='localhost', port=8668)
```

### Register time series data

Create and register time series data using QuantumLeapClient.

```python
body = {}
body["subscription_id"] = "5947d174793fe6f7eb5e3961" # Rondom value
entity = {}
entity["id"] = "Room1"
entity["type"] = "Room"
entity["temperature"] = {'type': 'Float', 'value': 23.0}
entity["pressure"] = {'type': 'Integer', 'value': 720}
body["data"] = [entity]

client.post_notify(body=body)
```


### Fetch time series data

Fetch time series data using QuantumLeapClient.

```python
response = client.get_entity_attribute(entity_id='Room1',
                                  attr_name='temperature')
print(response)
```

The resulting JSON looks like this :

```json
{
    "attrName": "temperature",
    "entityId": "Room1",
    "index": ["2021-01-29T01:44:33.198+00:00"],
    "values": ["23"]
}
```

API calls with parameters can be made by adding parameter names and values  
to the arguments of the QuantumLeapClient function.
In QuantumLeapClient, the parameters toDate and fromDate can handle python datetime objects.

```python
from datetime import datetime, timedelta

toDate = datetime.utcnow()
fromDate = toDate - timedelta(days=1)

response = client.get_entity(entity_id='Room1', toDate=toDate,
                             fromDate=fromDate, limit=1)
```

Quantum Leap can fetch up to 10000 data with one request.
By using QuantumLeapClient, it is possible to hide the loop processing 
when fetching more than 10000 data from QuantumLeap.

### Delete time series data

Delete time series data using QuantumLeapClient.

```python
client.delete_entity_id(entity_id='Room1')
```

## Correspondence table between API call and Quantum Leap Client

The QuantumLeapClient functions correspond to each [Quantum Leap API call](https://app.swaggerhub.com/apis/smartsdk/ngsi-tsdb/0.7).

| Quantum Leap endpoint | QuantumLeapClient function |
| :--- | :--- |
| GET    /version | get_version()    |
| POST   /config(Not implemented)  | post_config()    |
| GET    /health  | get_health()     |
| POST   /notify  | post_notify(body)|
| POST   /subscribe | post_subscription(orionUrl, quantumleapUrl) |
| DELETE /entities/{entityId} | delete_entity_id(entity_id) |
| DELETE /types/{entityType} | delete_entity_type(entity_type) |
| GET    /entities/{entityId}/attrs/{attrName} | get_entity_attribute(entity_id, attr_name) |
| GET    /entities/{entityId}/attrs/{attrName}/value | get_entity_attribute_value(entity_id, attr_name) |
| GET    /entities/{entityId} | get_entity(entity_id) |
| GET    /entities/{entityId}/value | get_entity_value(entity_id) |
| GET    /types/{entityType}/attrs/{attrName} | get_type_attribute(entity_type, attr_name) |
| GET    /types/{entityType}/attrs/{attrName}/value | get_type_attribute_value(entity_type, attr_name) |
| GET    /types/{entityType} | get_type(entity_type) |
| GET    /types/{entityType}/value | get_type_value(entity_type) |
| GET    /attrs/{attrName}         | get_attribute(attr_name) |
| GET    /attrs/{attrName}/value   | get_attribute_value(attr_name) |
| GET    /attrs | get_attrs() |
| GET    /attrs/value | get_attrs_value() |

QuantumLeap's API call P0ST / config hasn't been implemented yet, so post_config function
 isn't working properly.

## Licencse

[Apache 2.0](LICENSE)

## Attention

The QuantumLeap client library is running in the QuantumLeap version 0.7.6 environment.
API calls differ depending on the version of QuantumLeap, so QuantumLeapClient library may not work.
