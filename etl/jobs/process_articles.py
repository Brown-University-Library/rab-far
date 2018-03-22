columns = [
    'id','activity_report_id','article_type_id',
    'identifier','created_at','updated_at','title','journal',
    'number','volume','date','coauthors','book_status_id',
    'article_id_type_id','page_numbers'
    ]

def process(rabid, farData):
    return "{0} {1}".format(rabid, len(farData))