import requests
import ldap3
import json
import time
import sys
import csv
import argparse
import pymongo
import mysql.connector

from config.settings import config

def join_far_tables(dbConnection):
    query = """
        SELECT u.id AS bruid, u.email, r.id AS report_id
        FROM users AS u
        JOIN activity_reports AS r ON (u.id = r.user_id)
        JOIN consent_report AS c ON (c.activity_report_id = r.id)
        WHERE c.consent_type_id="library"
    """
    cursor = dbConnection.cursor()
    cursor.execute(query)
    data = [ row for row in cursor ]
    cursor.close()
    return data

def get_far_data(columns, table, cursor):
    # report_rows = get_far_data(
    #     ['id', 'user_id'], 'activity_reports', crsr)
    # consent_rows = get_far_data(
    #     ['activity_report_id','consent_type_id'],
    #     'consent_report', crsr)
    # user_rows = get_far_data(['*'],'users', crsr)
    query = "SELECT {0} FROM {1}".format(','.join(columns), table)
    cursor.execute(query)
    data = [ row for row in cursor ]
    return data

def filter_consenting_reports(rows):
    consent_idx = 1
    col_vals = { row[consent_idx] for row in rows }
    assert col_vals == { 'centers', 'library', 'ovpr', 'swearer' }
    return [ row for row in rows
                if row[consent_idx] == 'library' ]

def user_id_lookup(row, idPairs):
    bruid_idx = 0
    try:
        res = mongoColl.find_one({'bruid' : row[bruid_idx]})
        shortid = res['shortid']
        return (True, row)
    except:
        return (False, row)

def get_rab_identites(mongoColl):
    try:
        cursor = mongoColl.find({'bruid' : {'$ne' : ''} },
            {'_id': False, 'bruid': True, 'rabid': True})
        id_pairs = [ (doc['bruid'], doc['rabid']) for doc in cursor ]
        cursor.close()
    except:
        raise
    return id_pairs

def map_ids(farRows, mongoIds):
    id_map = { bruid : rabid for bruid, rabid in mongoIds }
    mapped = [ (id_map[bruid], bruid, email, report_id)
        for bruid, email, report_id in farRows if bruid in id_map ]
    return mapped

def filter_report_data(consentRows, reportRows):
    report_idx = 0
    rprt_id_idx = 0
    consenting_report_ids = { row[report_idx] for row in consentRows }
    return [ row for row in reportRows
                if row[rprt_id_idx] in consenting_report_ids]

def process():
    cnx = mysql.connector.connect(
        user=config['MYSQL_USR'], password=config['MYSQL_PASSW'],
        database=config['MYSQL_FAR'])
    mongo = pymongo.MongoClient(config['MONGO_ADDR'])
    id_db = mongo.get_database(config['MONGO_IDS'])
    id_coll = id_db['rabids']
    user_rows = join_far_tables(cnx)
    rabids = get_rab_identites(id_coll)
    rab_users = map_ids(user_rows, rabids)
    queryable_tables = ['articles', 'books', 'chapters',
        'papers', 'patents']
    # print(len(bad))
    # print(bad[:10])
    # print(len(shortids))
    # print(shortids[:10])
    # cnst_data = filter_consenting_faculty(consent_rows)
    # allowed_reports = filter_consenting_reports(
    #     cnst_data, report_rows)
    # consenting = get_user_data(cnst_data, report_rows, user_rows)
    cnx.close()
    mongo.close()
    return rab_users

if __name__ == '__main__':
    pass