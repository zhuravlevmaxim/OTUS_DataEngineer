import logging
  
import aerospike
from aerospike import predicates as p
import sys

config = {
  'hosts': [('127.0.0.1', 3000)]
}


try:
    client = aerospike.client(config).connect()
except:
    logging.error("failed to connect to the cluster with", config['hosts'])
    sys.exit(1)

try:
    client.index_string_create("test", "demo", "phone", "phone_indx")
except:
    logging.info("index created!")


def add_customer(customer_id, phone_number, lifetime_value):
    key = ("test", "demo", customer_id)
    client.put(key, {"phone": str(phone_number), "ltv": lifetime_value})


def get_ltv_by_id(customer_id):
    key = ("test", "demo", customer_id)
    (key_tmp, meta, store) = client.get(key)
    if (store == {}):
        logging.error("Requested non-existent customer " + str(customer_id))
    else:
        return store.get("ltv")


def get_ltv_by_phone(phone_number):

    query = client.query("test", "demo")
    query.select("ltv")
    query.where(p.equals("phone", phone_number))
    ltvs = []

    def matches_phone(tuple_1):
        key, metadata, bins = tuple_1
        ltvs.append(bins["ltv"])

    query.foreach(matches_phone, {"total_timeout": 2000})

    return ltvs[0]
