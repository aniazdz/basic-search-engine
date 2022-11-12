import json
import os
import re
from pathlib import Path
from bs4 import *
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer, word_tokenize
import ssl



class Indexer:
    def __init__(self):
        self.directory = "ANALYST"
        self.download_nltk_dependency()
        # index: { {token: {doc_id: weight}} }
        self.index = dict()
        self.docid_document_map = {}
        self.file_list = []
        self.doc_id = 1
        self.file_number_threshold = 2000
        self.batch_id = 1
            
    
    def download_nltk_dependency(self):
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context

        if not os.path.exists('./nltk_data'):
            os.mkdir('./nltk_data')
        nltk.data.path.append('./nltk_data/')
        nltk.download('punkt', download_dir='./nltk_data/')
        
                    
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
        self.add_docid_document_app()              
        
    
    def merge_index_batches(self, index_batch_dir):
        batches = Path(index_batch_dir).rglob('*.txt')
        pass
        
        
    def get_file_list(self):
        return Path(self.directory).rglob('*.json')
        
    
    def process_file_weights(self, webpage_json):
        url = webpage_json["url"]
        content = webpage_json["content"]
        soup = BeautifulSoup(content, features="xml")
        text = soup.text
        titles = ''
        headings = ''
        bolds = ''
        
        for title in soup.find_all('title'):
            titles += title.text+ ','
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            headings += heading.text + ','
        for bold in soup.find_all(['b', 'strong']):
            bolds += bold.text + ','
        
        return url, text, titles, headings, bolds

    
    def tokenize(self, text_str: str) -> list:
        # pattern = '^[a-zA-Z]+$'
        # token_list = []
        # word_list = re.split(r'[`!@#$%^&*()_+\-=\[\]{};\':“”\"\\|,.<>\/?~\s+]', text_str.lower())
        # for word in word_list:
        #     if re.match(pattern, word):
        #         token_list.append(word)
        #tokenizer = RegexpTokenizer('[a-zA-Z]+')
        #token_list = tokenizer.tokenize(text_str.lower())
        pattern = '^[a-z0-9]+$'
        token_list = word_tokenize(text_str.lower())
        token_list_beautify = []
        for token in token_list:
            if re.match(pattern, token):
                token_list_beautify.append(token)
        stemmer = SnowballStemmer("english")
        tokens = [stemmer.stem(token) for token in token_list_beautify]
        return tokens
    
    #adding up the frequencies of our tokens
    def compute_freq(self, token_list: list) -> dict:
        token_freq = dict()
        for token in token_list:
            if token in token_freq:
                token_freq[token] += 1
            else:
                token_freq[token] = 1
        return token_freq
    
    
    def write_local_batch(self):
        if not os.path.exists('./indexes'):
            os.mkdir('./indexes')
        with open(f'./indexes/index_batch_{self.batch_id}.txt', 'w') as batch_file:
            sorted_index = sorted(self.index.items(), key=lambda x: x[0])
            for index_token in sorted_index:
                batch_file.write(str(index_token) + '\n')
            batch_file.close()            
            
    
    
    def add_docid_document_app(self):
        if not os.path.exists('./indexes'):
            os.mkdir('./indexes')
        with open('./indexes/docid_url_map.json', 'w') as json_file:
            json.dump(self.docid_document_map, json_file)
        self.docid_document_map.clear()
        

#writing into the report with num of indexed docs, unique tockens, size of indexes.
def generate_report():
    with open('report_MS1.txt', 'w') as report_file:
        with open('./indexes/docid_url_map.json', 'r') as docid_json:
            docid_map = json.load(docid_json)
        report_file.write(f"The number of indexed documents: {len(docid_map)}\n\n")
        
        index_batches = Path('./indexes').rglob('*.txt')
        index_count = 0
        total_size_of_indexes = 0
        for batch in index_batches:
            index_file = open(batch, 'r')
            index_count += sum(1 for line in index_file if line.strip())
            total_size_of_indexes += os.path.getsize(batch) * 10**-6
        report_file.write(f"The number of unique tokens: {index_count}\n\n")
        report_file.write(f"The total size of indexes: {round(total_size_of_indexes, 3)}MB")
        report_file.close()
        
            
        
if __name__ == '__main__':
    indexer = Indexer()
    indexer.build_index()
    #indexer.merge_index_batches('./index')
    generate_report()



            
    
    
    