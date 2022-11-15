from indexer import Indexer
import os
import math
from pathlib import Path
import json
import re

class Query:
    def __init__(self, indexer):
        self.docid_document_dict = indexer.load_json('./indexes/docid_url_map.json')
        self.index = open('./indexes/index_batch_1.txt')
        self.token_posting_locations = indexer.load_json('./indexes/token_posting_locations.json')
        self.indexer = indexer
        self.query_tokens = []
        self.result_urls = list()
        
        
    def get_query_tokens(self):
        query = input('\nSearch Bar: ')
        self.query_tokens = self.indexer.tokenize(query)
        
        
    def ranking_retrieval(self):
        while not self.query_tokens:
            print("\nEmpty String is unacceptable!")
            self.get_query_tokens()
        
        token_posting_dict = dict() # {token: {doc_id: freq}}
        docid_score_dict = dict()
        
        for token in self.query_tokens:
            if token not in token_posting_dict:
                posting = self.get_token_posting(token) #{doc_id: freq}
                token_posting_dict[token] = posting
        token_posting_dict = dict(sorted(token_posting_dict.items(), key=lambda x: len(x[1])))
        
        for token, posting in token_posting_dict.items():
            idf = math.log(len(self.docid_document_dict) / len(posting))
            for docid, freq in posting.items():
                tf = 1 + math.log(freq)
                if docid not in docid_score_dict:
                    docid_score_dict[docid] = 0
                docid_score_dict[docid] += tf * idf
        
        docid_score_dict = dict(sorted(docid_score_dict.items(), key=lambda x: x[1], reverse=True))
        count_of_ranked_docs = 5
        doc_ids = list(docid_score_dict.keys())
        for i in range(count_of_ranked_docs):
            result_url = self.docid_document_dict[str(doc_ids[i])]
            self.result_urls.append(result_url)
                         
    
    def result(self):
        for i in range(len(self.result_urls)):
            print(f"\n{i + 1}. {self.result_urls[i]}")
            
            
    def get_token_posting(self, token):
        pointer = self.token_posting_locations[token]
        self.index.seek(pointer)
        line = self.index.readline()
        line_beautified = re.sub(r"[\'\(\)\{\}\:,]", "", line.strip())
        l = line_beautified.split()
        token = l[0]
        docid_freq_map = dict()
        for i in range(1, len(l), 2):
            docid_freq_map[int(l[i])] = float(l[i + 1])
        return docid_freq_map
        
        
    
        
if __name__ == '__main__':
    query = Query(Indexer())
    query.get_query_tokens()
    query.ranking_retrieval()
    query.result()
    
    """
    check this url if you cannot input your search string in VSCode: https://www.youtube.com/watch?v=mqp98TSVJUE
    """
    