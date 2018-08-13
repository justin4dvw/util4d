from pymongo import MongoClient
import sys
import urllib
sys.path.append('../treb_py')
from .config_loader import load_credentials
import logging


def connect_to_mongo(credential_file=None):
    _c = load_credentials(credential_file)
    if credential_file:
        _c['password']=urllib.quote(_c['password'])
        _c['cred']='{username}:{password}@'.format(**_c)
    else:
        _c['cred']=''

    address='mongodb://{cred}{ServerAddress}:{port}'.format(**_c)

    client=MongoClient(address)

    return client

def is_dupe(db, collection, record, keys=None):

    if isinstance(keys, list):
        _keys = {i:record[i] for i in keys}
    if isinstance(keys, str):
        _keys = {keys:record[keys]}
    else:
        keys=record
    if db[collection].count_documents(_keys) > 0:
        return True
    else:
        return False


def insert_record(db,collection,record, keys=None):
    if isinstance(record, dict):
        if not is_dupe(db,collection,record, keys):
            db[collection].insert_one(record)
            return 1
    elif isinstance(record, list):
        records=[r for r in record if not is_dupe(db,collection,r, keys)]
        if len(records)>0:
            logging.info("Inserted {0} records using bulk option".format(len(records)))
            db[collection].insert_many(records)
        return len(records)
