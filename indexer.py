import json
import re
from pathlib import Path
from bs4 import *
from nltk.stem.snowball import SnowballStemmer



class Indexer:
    def __init__(self):
        self.directory = "DEV"
        # index: { {token: {doc_id: weight}} }
        self.index = {}
        self.docid_document_map = {}
        self.file_list = []
        self.doc_id = 1
        self.file_number_threshold = 2000
        self.batch_id = 1
    
        
    def build_index(self):
        file_list = self.get_file_list()
            
        visited_sites = set()
        for file in file_list:
            with open(file, 'r') as json_file:
                webpage_json = json.load(json_file)
            
            url, text, titles, headings, bolds = self.process_file_weights(webpage_json)
            
            if '#' in url:
                url = url[:url.index('#')]
            if url not in visited_sites:
                
                text_tokens = self.tokenize(text)     
                title_tokens = self.tokenize(titles)
                heading_tokens = self.tokenize(headings)
                bold_tokens = self.tokenize(bolds)
                
                # weights:
                # title token -> 5
                # heading token -> 3
                # bold/strong token -> 2
                # normal token -> 1
                token_weights_map = self.compute_freq(text_tokens)
                for title_token in title_tokens:
                    if title_token in token_weights_map.keys():
                        token_weights_map[title_token] += 4
                for heading_token in heading_tokens:
                    if heading_token in token_weights_map.keys():
                        token_weights_map[heading_token] += 2
                for bold_token in bold_tokens:
                    if bold_token in token_weights_map.keys():
                        token_weights_map[bold_token] += 1
                
                for token, weight in token_weights_map.items():
                    if token not in self.index:
                        self.index[token] = dict()
                    self.index[token][self.doc_id] = weight
                
                self.docid_document_map[self.doc_id] = url
                self.doc_id += 1
                visited_sites.add(url)
            
            if (self.doc_id % self.file_number_threshold == 0):
                self.write_local_batch()
                self.index.clear()
                self.batch_id += 1              
        
        self.write_local_batch()
        self.index.clear()
                       
        with open('./index/docid_url_map.json', 'w') as json_file:
            json.dump(self.docid_document_map, json_file)
        self.docid_document_map.clear()
        
                
    def get_file_list(self):
        return Path(self.directory).rglob('*.json')
        
    
    def process_file_weights(self, webpage_json):
        url = webpage_json["url"]
        content = webpage_json["content"]
        soup = BeautifulSoup(content, "html.parser")
        text = soup.get_text()
        titles = ''
        headings = ''
        bolds = ''
        
        for title in soup.find_all('title'):
            titles += title + ','
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            headings += heading + ','
        for bold in soup.find_all(['b', 'strong']):
            bolds += bold + ','
        
        return url, text, titles, headings, bolds
        
        
    
    def tokenize(text_str: str) -> list:
        pattern = '^[a-zA-Z0-9]+$'  # include the single quotation mark
        token_list = []
        word_list = re.split(r'[`!@#$%^&*()_+\-=\[\]{};\':“”\"\\|,.<>\/?~\s+]', text_str.lower())
        for word in word_list:
            if re.match(pattern, word):
                token_list.append(word)
        stemmer = SnowballStemmer("english")
        tokens = [stemmer.stem(token) for token in token_list]
        return tokens
    
    
    def compute_freq(token_list: list) -> dict:
        token_freq = dict()
        for token in token_list:
            if token in token_freq:
                token_freq[token] += 1
            else:
                token_freq[token] = 1
        return token_freq
    
    
    def write_local_batch(self):
        with open(f'./indexes/index_batch_{self.batch_id}.txt', 'w') as batch_file:
            for index_token in sorted(self.index.items(), key=lambda x: x[0]):
                batch_file.write(index_token + '\n')   
            batch_file.close()            
    
    
    def update_url_list(self, visited_sites):
        for visited_site in visited_sites:
            .remove(visited_site)
            
    
    
                   
if __name__ == '__main__':
    indexer = Indexer()
    indexer.build_index()
    indexer.merge_index_batches('./index')
                
    
    
    