import pymongo

from config.settings import config

def get_rab_identites():
    mongo = pymongo.MongoClient(config['MONGO_ADDR'])
    id_db = mongo.get_database(config['MONGO_IDS'])
    try:
        cursor = id_db['rabids'].find(
            {'bruid' : {'$ne' : ''} },
            {'_id': False, 'bruid': True,
            'rabid': True, 'shortid': True } )
        id_pairs = [ (doc['bruid'], doc['rabid'], doc['shortid'])
            for doc in cursor ]
        cursor.close()
    except:
        raise
    return id_pairs