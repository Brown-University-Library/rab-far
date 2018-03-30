import requests
import csv
import time

from config.settings import config

rdf = '<http://www.w3.org/2000/01/rdf-schema#{}>'
bcite = '<http://vivo.brown.edu/ontology/citation#{}>'

def columns():
    return [ 'id','activity_report_id','article_type_id',
        'identifier','created_at','updated_at','title','journal',
        'number','volume','date','coauthors','book_status_id',
        'article_id_type_id','page_numbers'
        ]

def rab_schema():
    return [ bcite.format('title'), bcite.format('hasVenue'),
        bcite.format('pages'), bcite.format('number'),
        bcite.format('doi'), rdf.format('type'),
        bcite.format('volume'), bcite.format('date'),
        bcite.format('authorList')
    ]


def schema_map():
    return  {
        'article_type_id' : rdf.format('type'),
        'title' : bcite.format('title'),
        'journal' : bcite.format('hasVenue'),
        'number' : bcite.format('number'),
        'volume' : bcite.format('volume'),
        'date' : bcite.format('date'),
        'page_numbers' : bcite.format('pages'),
        'pmid' : bcite.format('pmid'),
        'doi' : bcite.format('doi'),
        'coauthors' : bcite.format('authorList')
    }

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
    time.sleep(0.3)
    resp = requests.post(rab_url, params=payload, headers=headers)
    if resp.status_code == 200:
        rdr = csv.DictReader( resp.text.splitlines() )
        data = [ row for row in rdr ]
    else:
        data = []
    return data

def filter_known_articles(rabData, farData):
    known_ids = { row['doi'] for row in rabData }
    known_ids &= { row['pmid'] for row in rabData }
    unknown_ids = [ row for row in farData
        if row['identifier'] != ''
        and row['identifier'] not in known_ids ]
    return unknown_ids

def map_article_identifier(row):
    if row['article_id_type_id'] == 'DOI':
        row['doi'] = row['identifier']
    elif row['article_id_type_id'] == 'PUDBMED':
        row['pmid'] = row['identifier']
    return row

def map_citation_types(row):
    far_type = row[rdf.format('type')]
    if far_type == 'BOOK':
        row[rdf.format('type')] = bcite.format('Review')
    elif far_type == 'CONF':
        row[rdf.format('type')] = bcite.format('ConferencePaper')
    elif far_type == 'PEER' or far_type == 'NONPEER':
        row[rdf.format('type')] = bcite.format('Article')
    else:
        row[rdf.format('type')] = bcite.format('Citation')
    return row

def cast_far_data_to_rab_schema(rows, schemaMap):
    mapped_id = [ map_article_identifier(row) for row in rows ]
    recast = [ { schemaMap[k] : v for k,v in row.items()
                    if k in schemaMap }
        for row in mapped_id ]
    mapped_type = [ map_citation_types(row) for row in recast ]
    return mapped_type

def clean_from_manager(rabid, approvedIDs):
    pass

def convert_to_rdf(rabid, rows):
    pass

def add_column_names(rows, colNames):
    return [ { z[0] : z[1] for z in zip(colNames, row) }
        for row in rows ]

def process(rabid, farData):
    rab_ids = rab_article_identifiers(rabid)
    labelled_data = add_column_names(farData, columns())
    new_articles = filter_known_articles(rab_ids, labelled_data)
    return cast_far_data_to_rab_schema(new_articles, schema_map())