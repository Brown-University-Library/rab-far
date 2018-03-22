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

def mysql_query(func):

    def query(*args, **kwargs):
        try:
            cnx = mysql.connector.connect(
                user=config['MYSQL_USR'], password=config['MYSQL_PASSW'],
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
def get_consenting_reports():
    return """
        SELECT u.id AS bruid, u.email, r.id AS report_id
        FROM users AS u
        JOIN activity_reports AS r ON (u.id = r.user_id)
        JOIN consent_report AS c ON (c.activity_report_id = r.id)
        WHERE c.consent_type_id="library"
    """
@mysql_query
def get_far_table(table, columns=['*']):
    return "SELECT {0} FROM {1}".format(','.join(columns), table)


def get_rab_identites():
    mongo = pymongo.MongoClient(config['MONGO_ADDR'])
    id_db = mongo.get_database(config['MONGO_IDS'])
    try:
        cursor = id_db['rabids'].find({'bruid' : {'$ne' : ''} },
            {'_id': False, 'bruid': True, 'rabid': True})
        id_pairs = [ (doc['bruid'], doc['rabid']) for doc in cursor ]
        cursor.close()
    except:
        raise
    return id_pairs

def map_farids_to_rabids(farRows, rabIds):
    id_map = { bruid : rabid for bruid, rabid in rabIds }
    mapped = [ (id_map[bruid], bruid, email, report_id)
        for bruid, email, report_id in farRows if bruid in id_map ]
    return mapped

def process():
    user_reports = get_consenting_reports()
    rabids = get_rab_identites()
    reports_with_rabids = map_farids_to_rabids(user_reports, rabids)

    return reports_with_rabids

if __name__ == '__main__':
    pass