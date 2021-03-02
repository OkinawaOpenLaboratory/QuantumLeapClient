# Quantum Leap Client Library for Python

## Install

```bash
pip install -r requirements.txt
```

```bash
python setup.py install
```

## Usage

```python
from quantumleapclient.client import Client
```

## What is it?
 'QuantumLeapClient' is a Python client library  
that helps QuantumLeap API calls.  
## Key Features

- Quantum Leap version > 0.7.6 
- [NGSI v2](https://fiware.github.io/specifications/ngsiv2/stable) support
- Handle [QuantumLeap API calls](https://app.swaggerhub.com/apis/smartsdk/ngsi-tsdb/0.7) in python
- Handle python datetime object as a parameter
- Get all the data stored in QuantumLeap without infinite loop processing
- Allow visualization/debbugging facilities

## Getting started

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

### Delete time series data

Delete time series data using QuantumLeapClient.

```python
client.delete_entity_id(entity_id='Room1')
```

## Dependencies
- logging
- json
- requests

## Licencse

[Apache 2.0](LICENSE)

## Attention

The QuantumLeap client library is running in the QuantumLeap version 0.7.6 environment.
API calls differ depending on the version of QuantumLeap, so QuantumLeapClient library may not work.
