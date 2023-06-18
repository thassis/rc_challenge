from typing import Dict, List, Sequence

from whoosh import qparser
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import MultifieldParser
from whoosh.filedb.filestore import RamStorage
from whoosh.analysis import StemmingAnalyzer

import json, csv

#
# Simple example indexing to an in-memory index and performing a search
# across multiple fields and returning an array of highlighted results.
#
# One lacking feature of Whoosh is the no-analyze option. In this example
# the SearchEngine modifies the given schema and adds a RAW field. When doc
# are added to the index only stored fields in the schema are passed to Whoosh
# along with json encoded version of the whole doc stashed in the RAW field.
#
# On query the <Hit> in the result is ignored and instead the RAW field is
# decoded containing any extra fields present in the original document. 
#

TOP_K = 10

class SearchEngine:

    def __init__(self, schema):
        self.schema = schema
        schema.add('raw', TEXT(stored=True))
        self.ix = RamStorage().create_index(self.schema)

    def index_documents(self, docs: Sequence):
        writer = self.ix.writer()
        for doc in docs:
            d = {k: v for k,v in doc.items() if k in self.schema.stored_names()}
            d['raw'] = json.dumps(doc) # raw version of all of doc
            writer.add_document(**d)
        writer.commit(optimize=True)

    def get_index_size(self) -> int:
        return self.ix.doc_count_all()

    def query(self, q: str, fields: Sequence, highlight: bool=True) -> List[Dict]:
        search_results = []
        with self.ix.searcher() as searcher:
            og = qparser.OrGroup.factory(0.9)
            results = searcher.search(MultifieldParser(fields, schema=self.schema, group=og).parse(q))
            if len(results) > 0:
                print(len(results))
            for r in results:
                d = json.loads(r['raw'])
                if highlight:
                    for f in fields:
                        if r[f] and isinstance(r[f], str):
                            d[f] = r.highlights(f) or r[f]

                search_results.append(d)

        return search_results

if __name__ == '__main__':
    docs = []
    output = []

    with open("files/sample.jsonl", "r") as file:
        for line in file:
            json_obj = json.loads(line)
            docs.append(json_obj)

    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        text=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        keywords=KEYWORD(stored=True)
    )

    engine = SearchEngine(schema)
    engine.index_documents(docs)

    print(f"indexed {engine.get_index_size()} documents")

    fields_to_search = ["title", "text", "keywords"]

    with open("files/test_queries.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            query_id = row["QueryId"]
            query = row["Query"]
            entity_ids = []

            results = engine.query(query, fields_to_search, highlight=True)
            if len(results)>0:
                print(results)
            #pecorre results até no máximo os TOP_K primeiro
            for i in range(min(TOP_K, len(results))):
                print("id: ", results[i]["id"])
                entity_ids.append(results[i]["id"])
                print("-"*70)
                if results[i]["id"]:
                    output.append({"QueryId": query_id, "EntityId": results[i]["id"]})

        output_file = "output.csv"

with open(output_file, "w", newline="") as csvfile:
    fieldnames = ["QueryId", "EntityId"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(output)
        