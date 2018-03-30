import mysql.connector
from config.settings import config

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