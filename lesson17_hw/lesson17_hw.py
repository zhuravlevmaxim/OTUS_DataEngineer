import logging

import aerospike
import sys

config = {
    'hosts': [('127.0.0.1', 3000)]
}

try:
    client = aerospike.client(config).connect()
except:
    print("failed to connect to the cluster with", config['hosts'])
    sys.exit(1)

key = ('test', 'demo', 'store')


def add_customer(customer_id, phone_number, lifetime_value):
    try:
        (key_tmp, meta, store) = client.get(key)
    except:
        logging.error('store not found')
        store = {}

    store[customer_id] = {'phone': phone_number, 'ltv': lifetime_value}
    print(store)
    client.put(key, store)


def get_ltv_by_id(customer_id):
    (key_tmp, meta, store) = client.get(key)
    item = store.get(customer_id, {})
    if (item == {}):
        logging.error('Requested non-existent customer ' + str(customer_id))
    else:
        return item.get('ltv')


def get_ltv_by_phone(phone_number):
    (key_tmp, meta, store) = client.get(key)
    for v in store.values():
        if (v['phone'] == phone_number):
            return v['ltv']
        logging.error('Requested phone number is not found ' + str(phone_number))
