import sys
import ast

from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED, DATETIME
from whoosh.filedb.filestore import FileStorage
from whoosh.writing import AsyncWriter
from whoosh import index, qparser, query



def index():
    companies = sys.argv[1]

    schema = Schema(crunch_url=ID(stored=True),
                    company_url=ID(stored=True),
                    category_list=STORED(),
                    region=TEXT(stored=True),
                    city=TEXT(stored=True),
                    state_code=ID(stored=True),
                    status=ID(stored=True))

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    st = FileStorage("indexdir").create()
    ix = st.create_index(schema)

    writer = AsyncWriter(ix)

    with open(companies, "r") as data:
        for line in data:
            temp = ast.literal_eval(line)
            writer.add_document(city=str(temp[9], "utf-8"), region=str(temp[8], "utf-8"), status=str(temp[5], "utf-8"))
        writer.commit()
        data.close()
    print ('indexing complete')


def search(page, queryStr, field):
    st = FileStorage("indexdir")
    ix = st.open_index()

    qp = qparser.QueryParser(None, schema=ix.schema, group=qparser.OrGroup)

    mask = query.Term("status", "closed")

    qp.add_plugin(qparser.MultifieldPlugin(field))
    qp.add_plugin(qparser.FuzzyTermPlugin())
    q = qp.parse(str(queryStr, "utf-8"))
    # print q

    with ix.searcher() as s:
        results = s.search_page(q, page, pagelen=20)
        for i in results:
            print (i)
        print ( len(results) )

if len(sys.argv) > 1:
    index()

search(1, "San Fransico", ["city", "region"])




#['permalink','name','homepage_url','category_list','funding_total_usd','status','country_code','state_code','region','city','funding_rounds']
