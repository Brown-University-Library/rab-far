import os
import sys
import pymongo
import mysql.connector

from config.settings import config
from jobs import process_articles

def mysql_query(func):
    def query(*args, **kwargs):
        try:
            cnx = mysql.connector.connect(
                user=config['MYSQL_USR'],
                password=config['MYSQL_PASSW'],
                database=config['MYSQL_FAR'])
            cursor = cnx.cursor()
            cursor.execute( func(*args, **kwargs) )
            data = [ row for row in cursor ]
            cursor.close()
            cnx.close()
        except:
            raise
        return data
    return query

@mysql_query
def get_consenting_reports(year=None):
    q = """
        SELECT u.id AS bruid, u.email,
          r.id AS report_id, r.year_id AS year
        FROM users AS u
        JOIN activity_reports AS r ON (u.id = r.user_id)
        JOIN consent_report AS c ON (c.activity_report_id = r.id)
        WHERE c.consent_type_id="library"
    """
    if year:
        q += ' AND r.year_id="{0}"'.format(year)
    return q

@mysql_query
def get_far_table(table, report_id=None, columns=['*']):
    q = 'SELECT {0} FROM {1}'.format(','.join(columns), table)
    if report_id:
        q += ' WHERE activity_report_id="{0}"'.format(report_id)     
    return q

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

def save_data(dataToLoad, report, table):
    load_dir = config['LOAD_DIR']
    os.makedirs( os.path.join(load_dir, report['shortid']) )
    file_name = "{0}_{1}_{2}.txt".format(
        report['report'], table, report['year'] )
    with open( os.path.join(
        load_dir, report['shortid'], file_name), 'w') as f:
        f.write(dataToLoad)

def map_farids_to_rabids(farRows, rabIds):
    id_map = { bruid : (rabid, shortid)
        for bruid, rabid, shortid in rabIds }
    mapped = [ { 'rabid': id_map[bruid][0], 'shortid': id_map[bruid][1],
                'bruid': bruid, 'email': email,
                'report': report_id, 'year': year }
        for bruid, email, report_id, year in farRows if bruid in id_map ]
    return mapped

def main(year=None,
    farTables=['articles','books','chapters','papers','patents']):
    job_map = {
        'articles' : process_articles,
        # 'books' : process_books,
        # 'chapters' : process_chapters,
        # 'papers' : process_papers,
        # 'patents' : process_patents
    } 
    user_reports = get_consenting_reports(year)
    rabids = get_rab_identites()
    reports_with_rabids = map_farids_to_rabids(user_reports, rabids)

    for report in reports_with_rabids:
        for table in farTables[:1]:
            table_job = job_map[table]
            far_data = get_far_table( table,
                columns=table_job.columns, report_id=report['report'])
            data_to_load = table_job.process(report['rabid'], far_data)
            save_data(data_to_load, report, table)
    return

if __name__ == '__main__':
    if len(sys.argv) > 1:
        year = sys.argv[1]
        main(year)
    else:
        main()
    