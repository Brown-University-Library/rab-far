import requests
import csv

from config.settings import config

columns = [
    'id','activity_report_id','article_type_id',
    'identifier','created_at','updated_at','title','journal',
    'number','volume','date','coauthors','book_status_id',
    'article_id_type_id','page_numbers'
    ]

def rab_article_identifiers(rabid):
    query = """
        PREFIX bcite:    <http://vivo.brown.edu/ontology/citation#>
        SELECT ?cite ?doi ?pmid
        WHERE
        {{
              <{0}> bcite:contributorTo ?cite .
              OPTIONAL {{ ?cite bcite:doi ?doi . }}
              OPTIONAL {{ ?cite bcite:pmid ?pmid . }}
        }}
    """.format(rabid)
    payload = {
        'email': config['RAB_USER'],
        'password': config['RAB_PASSW'],
        'query' : query
    }
    headers = { 'accept': 'text/csv' }
    rab_url = config['RAB_URL']
    resp = requests.post(rab_url, params=payload, headers=headers)
    if resp.status_code == 200:
        rdr = csv.DictReader( resp.text.splitlines() )
        data = [ row for row in rdr ]
    else:
        data = []
    return data

def filter_known_articles(rabData, farData, farColumns):
    known_ids = { row['doi'] for row in rabData }
    known_ids &= { row['pmid'] for row in rabData }
    unknown_ids = [ row for row in farData
        if row[farColumns.index('identifier')] not in known_ids ]
    return unknown_ids

def process(rabid, farData):
    rab_ids = rab_article_identifiers(rabid)
    new_articles = filter_known_articles(rab_ids, farData, columns)
    return "{0} {1}".format(rabid, len(new_articles))