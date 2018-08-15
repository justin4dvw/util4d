from util4d import *
import pandas as pd
import requests
from urllib.parse import quote
from .mongo_transactions import *

cursor=connect_to_mongo()


def retain_street_score(_dict, key, data):

    _dict[key]={'walk_score':data['walk_score'],
               'transit':{}}
    if 'transit' in data:
        _dict[key]['transit'] = data['transit']
    return _dict

def get_street_score(_dict, key):
    if key in _dict.keys():
        return _dict[key]
    else:
        return False

def get_street_info(cursor, db, collection):
    return [i for i in cursor[db][collection].find()]

def get_walk_score(record, street_info, api_key, lat_column='latitude', lon_column='longitude',
                    address_column='street_name', city_column=None, country_column=None, default_city_name=None,
                    default_country_name=None
                    ):

    if not city_column and not default_city_name:
        raise ValueError("You have to provide either city_column or default_city_name")

    if not country_column and not default_country_name:
        raise ValueError("You have to provide either country_column or default_country_name")
    url_format="http://api.walkscore.com/score?format=json&{encoded_input}&transit=1&bike=1"

    if 'walk_score' in record:
        return {'status':True,
                'fetched':False,
                'walk_score':record['walk_score'],
                'transit_score':record['transit_score'],
                'limit_exceeded':False,
                'message':'Record already has score'
                }

    if 'street_name' not in record:
        return {'status':None,
                'fetched':False,
                'walk_score':None,
                'transit_score':None,
                'limit_exceeded':False,
                'message':'No record found for {}'.format(address_column)
                }

    adr = {
            'street_name': record[address_column],
            'city_name': default_city_name if  default_city_name else record['default_city_name'],
            'country_name': default_country_name if  default_country_name else record['default_country_name']
            }

    address = '{street_name}, {city_name}, {country_name}'.format(**adr)
    _score = get_street_score(street_info, address)

    if not _score:
        # call api
        key_format = {'lat': record['latitude'],
                      'lon': record['longitude'],
                      'address': address,
                      'wsapikey': api_key
                     }
        encoded_input = urllib.urlencode(key_format)
        url = url_format.format(encoded_input=encoded_input)
        r = requests.get(url)

        if r.status_code==200:
            result = r.json()
            if result['status'] == 1:
                record['walk_score'] = result['walkscore']
                if 'transit' in result:
                    record['transit'] = result['transit']
                else:
                    record['transit'] = None

                    return {'status':True,
                            'fetched':True,
                            'limit_exceeded':False,
                            'walk_score':record['walk_score'],
                            'transit_score':record['transit'],
                            'message':'Success'
                            }

            elif result['status'] == 41:
                within_daily_call_limit = False
                return {'status':None,
                        'fetched':False,
                        'limit_exceeded':True,
                        'walk_score':None,
                        'transit_score':None,
                        'message':'Daily limit exceeded for API'
                        }
        else:
            return {'status':None,
                    'fetched':False,
                    'limit_exceeded':False,
                    'walk_score':None,
                    'transit_score':None,
                    'message':'Received http response {0}'.format(r.status_code)
                    }
    else:
        return {'status':True,
                'fetched':False,
                'limit_exceeded':False,
                'walk_score':_score['walk_score'],
                'transit_score':_score['transit'],
                'message':'Daily limit exceeded for API'
                }
