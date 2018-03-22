import os
import sys
import csv
import datetime
import pymongo
import logging

from config import settings
logging.basicConfig(filename=settings['LOGFILE'],level=logging.DEBUG)

mongo = pymongo.MongoClient(settings['MONGO'])
far_db = mongo.get_database(settings['MONGODB'])
# far_coll = far_db[ 'sqldump' ]

def parse_datetime(rawDate):
    out = {}
    try:
        return datetime.datetime.strptime(rawDate, '%Y-%m-%d %H:%M:%S')
    except:
        return False

def main(dataDir):
    csv_files = os.listdir(dataDir)
    for filename in csv_files:
        print(filename)
        with open(os.path.join(dataDir, filename),'r') as f:
            rdr = csv.DictReader(f, escapechar="\\")
            data = []
            for row in rdr:
                # row['table'] = filename.split('.csv')[0]
                far_coll = far_db[ filename.split('.csv')[0] ]
                sql_id = row.get('id')
                if sql_id:
                    row['sql_id'] = sql_id
                    del row['id']
                upd = row.get('updated_at')
                if upd:
                    row['updated_at'] = parse_datetime(upd)
                ctd = row.get('created_at')
                if ctd:
                    row['created_at'] = parse_datetime(ctd)
                data.append(row)
            if data:
                far_coll.insert_many(data)
            else:
                print("EMPTY: {}".format(filename))
                continue

if __name__ == '__main__':
    data_dir = sys.argv[1]
    main(data_dir)