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

- [NGSI v2](https://fiware.github.io/specifications/ngsiv2/stable) support
- Handle [QuantumLeap API calls](https://app.swaggerhub.com/apis/smartsdk/ngsi-tsdb/0.7) in python
- Handle python datetime object as a parameter
- Get all the data stored in QuantumLeap without infinite loop processing
- Allow visualization/debbugging facilities

## Where to get it
The source code is currently hosted on GitHub at :
https://github.com/OkinawaOpenLaboratory/QuantumLeapClient

## Getting started
### Connect QuantumLeap

Connect QuantumLeap

```python
from quantumleapclient.client import Client

m = Client()
```

### POST QuantumLeap data

```python
from quantumleapclient.client import Client

body = {}
subscription_id = "5947d174793fe6f7eb5e3961"
body["subscription_id"] = subscription_id
dataList = []
data = {}
entity_id = "Room1"
entity_type = "Room"
data["id"] = entity_id
data["type"] = entity_type
attribute_temperature = {}
attribute_temperature["value"] = 23.0
attribute_temperature["type"] = "Float"
data["temperature"] = attribute_temperature
attribute_pressure = {}
attribute_pressure["value"] = 720
attribute_pressure["type"] = "Integer"
data["pressure"] = attribute_pressure
dataList.append(data)
body["data"] = dataList

m = Client()
response = m.post_notify(body=body)
```


### GET QuantumLeap data

```python
from quantumleapclient.client import Client

m = Client()
response = m.get_entity_attribute(entity_id='Room1',
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

### DELETE QuantumLeap data

```python
from quantumleapclient.client import Client

m = Client()
response = m.delete_entity_id(entity_id='Room1')
```

## Dependencies

## Licencse

## Background
