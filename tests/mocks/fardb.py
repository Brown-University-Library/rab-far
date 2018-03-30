import os
import csv
import mysql.connector

from etl.jobs import process_articles
from config.settings import config

data_dir = os.path.join(config['TEST_DIR'], 'data')

def get_test_data():
    cnx = mysql.connector.connect(
        user=config['MYSQL_USR'],
        password=config['MYSQL_PASSW'],
        database=config['MYSQL_FAR'])
    cursor = cnx.cursor()
    q = """
        SELECT DISTINCT u.id AS bruid, u.email,
          r.id AS report_id, r.year_id AS year
        FROM users AS u
        JOIN activity_reports AS r ON (u.id = r.user_id)
        JOIN consent_report AS c ON (c.activity_report_id = r.id)
        JOIN articles AS a on (a.activity_report_id = r.id)
        WHERE c.consent_type_id="library"
        LIMIT 20
    """
    cursor.execute( q )
    data = [ row for row in cursor ]
    with open(os.path.join(data_dir, 'reports.csv'), 'w') as f:
        writer = csv.writer(f)
        for d in data:
            writer.writerow(d)
    # for d in data:
    #     q = """
    #         SELECT {0} FROM {1}
    #         WHERE activity_report_id="{2}"
    #     """.format(','.join(process_articles.columns()),
    #         'articles', d[2])
    #     cursor.execute( q )
    #     data = [ row for row in cursor ]
    #     file_name = 'articles-{0}.csv'.format(d[2])
    #     with open(os.path.join( data_dir, file_name), 'w') as f:
    #         writer = csv.writer(f)
    #         for d in data:
    #             writer.writerow(d)
    cursor.close()
    cnx.close()

def get_consenting_reports(year=None):
    with open(os.path.join(data_dir, 'reports.csv'), 'r') as f:
        rdr = csv.reader(f)
        rows = [ row for row in rdr ]
    return rows

# def get_far_table(table, report_id=None, columns=['*']):
#     q = 'SELECT {0} FROM {1}'.format(','.join(columns), table)
#     if report_id:
#         q += ' WHERE activity_report_id="{0}"'.format(report_id)     
#     return q