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
        
        query_freq = self.indexer.compute_freq(self.query_tokens) # {token: freq}
        docid_token_score_dict = dict() # {docid: {token: score}}
        query_score_dict = dict() # {query_token: score}
        
        
        query_normalizer = 0
        for token, freq_query in query_freq.items():
            posting = self.get_token_posting(token)
            query_score_dict[token] = (1 + math.log10(freq_query)) * math.log10(len(self.docid_document_dict) / len(posting))
            
            for docid, freq in posting.items():
                if docid not in docid_token_score_dict:
                    docid_token_score_dict[docid] = dict()
                docid_token_score_dict[docid][token] = (1 + math.log10(freq)) * math.log10(len(self.docid_document_dict) / len(posting))
            
            query_normalizer += query_score_dict[token] * query_score_dict[token]
        query_normalizer = math.sqrt(query_normalizer)
        for token in query_score_dict.keys():
            query_score_dict[token] /= query_normalizer
        
        
        for docid, token_score_map in docid_token_score_dict.items():
            doc_normalizer = 0
            for score in token_score_map.values():
                doc_normalizer += score * score
            doc_normalizer = math.sqrt(doc_normalizer)
            for score in token_score_map.values():
                score /= doc_normalizer
        
        
        docid_cosine_similarity_dict = dict()
        for docid, token_score_map in docid_token_score_dict.items():
            similarity = 0
            for token, score in token_score_map.items():
                if token not in query_score_dict:
                    continue
                similarity += query_score_dict[token] * score
            docid_cosine_similarity_dict[docid] = similarity
        
        
        docid_cosine_similarity_dict = dict(sorted(docid_cosine_similarity_dict.items(), key=lambda x: x[1], reverse=True))
        count_of_ranked_docs = 5
        doc_ids = list(docid_cosine_similarity_dict.keys())
        if len(doc_ids) < count_of_ranked_docs:
            for i in range(len(doc_ids)):
                result_url = self.docid_document_dict[str(doc_ids[i])]
                self.result_urls.append(result_url)
        else:
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
    