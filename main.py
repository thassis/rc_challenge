import pyterrier as pt
import pandas as pd
import shutil
import json
import csv

pt.init()

TOP_K = 100

output = []

folder_path = 'index'  # Replace with the actual folder path
# shutil.rmtree(folder_path)

jsonl_file = "files/corpus.jsonl"

# def iter_file(filename):
#   with open(filename, 'rt') as file:
#     for l in file:
#       # assumes that each line contains 'docno', 'text' attributes
#       # yields a dictionary for each json line
#       obj = json.loads(l)
#       obj["docno"] = obj["id"]
#       yield obj

# indexref4 = pt.IterDictIndexer("./index", meta={'docno': 20, 'text': 4096}).index(iter_file(jsonl_file))

# Load the index
index_path = './index'  # Replace with the actual index folder path
index = pt.IndexFactory.of(index_path)

tokenizer = pt.autoclass("org.terrier.indexing.tokenisation.Tokeniser").getTokeniser()
def strip_markup(text):
    return " ".join(tokenizer.getTokens(text))

with open("files/test_queries.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        query_id = row["QueryId"]
        query = row["Query"]
        entity_ids = []

        # Retrieve documents based on the query
        retrieval = pt.BatchRetrieve(index, controls={"wmodel": "BM25"})
        print(strip_markup(query))
        results = retrieval.search(strip_markup(query))

        # Print the document IDs and scores
        for doc_id, score in zip(results["docno"], results["score"]):
            print(f"Document ID: {doc_id}, Score: {score}")
            entity_ids.append(doc_id)

        if len(entity_ids) > 0:
            for i in range(min(TOP_K, len(entity_ids))):
                output.append({"QueryId": query_id, "EntityId": entity_ids[i]})

with open('output_file.csv', "w", newline="") as csvfile:
    fieldnames = ["QueryId", "EntityId"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(output)

print("-"*70)