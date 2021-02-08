from quantumleapclient.client import Client

m = Client(host="localhost", port=8668)
resp = m.get_entity_attribute(entity_id='Room1', attr_name='temperature')

print(resp)
