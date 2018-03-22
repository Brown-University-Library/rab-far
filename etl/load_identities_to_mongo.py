import sys
import json
import pymongo
from datetime import datetime

from config import settings

mongo = pymongo.MongoClient(settings['MONGO'])
id_db = mongo.get_database(settings['RABDAP'])
id_coll = id_db['rabids']

def main(inFile):
    with open(inFile, 'r') as f:
        jdata = json.load(f)

    data = []
    for j in jdata:
        id_map = {}
        id_map['bruid'] = j['brown_id']
        id_map['shortid'] = j['short_id']
        id_map['email'] = j['email']
        id_map['rabid'] = 'http://vivo.brown.edu/individual/{}'.format(
            j['short_id'])
        id_map['created'] = datetime.now()
        id_map['updated'] = datetime.now()
        id_map['historical'] = {}
        data.append(id_map)

    id_coll.insert_many(data)

if __name__ == '__main__':
    in_file = sys.argv[1]
    main(in_file)