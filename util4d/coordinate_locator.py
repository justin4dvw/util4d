from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from .config_loader import load_mapping
from .mongo_transactions import connect_to_mongo


def geo_parser(record, geo_column = 'geo_polygons'):
    locs = []
    for x in record[geo_column]:
        if 'lat' in x and 'lon' in x:
            locs.append((x['lat'], x['lon']))
    record['coordinates'] = locs
    return record


def get_location_based_on_coordinate(record, record_lat_column, record_lng_column, mapping_file = 'region_location.yaml', cred_file = None, db=None,  collections=None):

    if db and collections:
        db = db
        if isinstance(collections,str):
            collections=[collections]
        elif isinstance(collections, list):
            collections=collections
        else:
            raise TypeError("Unexpected type for collections received. Expecting list or string")

    else:
        region_locations = load_mapping(mapping_file)
        db = region_locations['db']
        collections = region_locations['collections']

    conn = connect_to_mongo(cred_file)
    regions =[]

    for collection in collections:
        c = conn[db][collection]
        regions += [i for i in c .find()]

    point = Point(( record[record_lat_column], record[record_lng_column]))

    counter = 0
    while counter < len(regions):
        polygon = Polygon(geo_parser(regions[counter])['coordinates'])

        if polygon.contains(point):
            return regions[counter]['region']
        counter +=1
    return None
