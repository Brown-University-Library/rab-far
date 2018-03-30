import os
import csv
import sys

from config.settings import config

from utils import fardb, iddb
from etl.jobs import process_articles

def save_data(dataToLoad, schema, report, table):
    load_dir = config['LOAD_DIR']
    faculty_dir = os.path.join(load_dir, report['shortid'])
    if not os.path.isdir( faculty_dir ):
        os.makedirs( faculty_dir )
    file_name = "{0}_{1}_{2}.csv".format(
        report['report'], table, report['year'] )
    with open( os.path.join(faculty_dir, file_name), 'w') as f:
        writer = csv.DictWriter(f, fieldnames=schema)
        writer.writeheader()
        for row in dataToLoad:
            writer.writerow(row)

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
    user_reports = fardb.get_consenting_reports(year)
    rabids = iddb.get_rab_identites()
    reports_with_rabids = map_farids_to_rabids(user_reports, rabids)

    for report in reports_with_rabids:
        for table in farTables[:1]:
            table_job = job_map[table]
            far_data = fardb.get_far_table( table,
                columns=table_job.columns(), report_id=report['report'])
            data_to_load = table_job.process(report['rabid'], far_data)
            save_data(
                data_to_load, table_job.rab_schema(), report, table)
    return

if __name__ == '__main__':
    if len(sys.argv) > 1:
        year = sys.argv[1]
        main(year)
    else:
        main()
    