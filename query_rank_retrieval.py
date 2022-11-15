from indexer import Indexer
import os
import math
from pathlib import Path
import json
import re

class Query:
    def __init__(self, indexer):
        self.docid_document_dict = json.load(open('./indexes/docid_url_map.json', 'r'))
        self.index = open('./indexes/index_batch_1.txt')
        self.token_posting_locations = json.load(open('./indexes/token_posting_locations.json', 'r'))
        self.query_tokens = []
        
        
    def get_query_tokens(self):
        query = input('\nSearch Bar: ')
        self.query_tokens = Indexer.tokenize(query)
        
        
    def ranking_retrieval(self):
        if self.query_tokens:
            tf_query = Indexer.compute_freq(self.query_tokens)
            query_tokens_scores = dict()
            docid_containing_query_tokens = dict()
            
            for token, freq in self.query_tokens.items():
                token_posting = self.get_token_posting(token)
                
                for docid, freq in token_posting.items():
                    if docid not in docid_containing_query_tokens:
                        docid_containing_query_tokens[docid] = dict()
                    docid_containing_query_tokens[docid][token] = freq
                    
                tf = 1 + math.log(freq)
                idf = math.log(len(self.docid_document_dict) / len(token_posting))
                query_tokens_scores[token] = tf * idf
                
                
            
    
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
    query_ranking = Query()
    