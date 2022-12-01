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
        self.directory = "DEV"
        self.download_nltk_dependency()
        # index: { {token: {doc_id: weight}} }
        self.index = dict()
        self.docid_document_map = {}
        self.file_list = []
        self.doc_id = 1
        self.file_number_threshold = 10000
        self.batch_id = 1
            
    
    def load_json(self, file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    
    
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
        
    
    def binary_merge_index_batches(self, index_batch_dir):
        batches = Path(index_batch_dir).rglob('*.txt')
        cur_temp = 0
        try:
            open('./indexes/temp0.txt', 'x')
            open('./indexes/temp1.txt', 'x')
        except FileExistsError:
            self.clear_merge_temp_files()
        
        for cur_batch in batches:
            cur_batch_file = open(cur_batch, 'r')
            if cur_temp == 0:
                input_temp = open('./indexes/temp0.txt', 'r')
                output_temp = open('./indexes/temp1.txt', 'w')
            elif cur_temp == 1:
                input_temp = open('./indexes/temp1.txt', 'r')
                output_temp = open('./indexes/temp0.txt', 'w')
            
            line1 = cur_batch_file.readline().strip('\n')
            line2 = input_temp.readline().strip('\n')
            while True:
                if line1 == '':
                    while True:
                        if line2 == '':
                            break
                        output_temp.write(line2 + '\n')
                        line2 = input_temp.readline().strip('\n')
                    break
                
                if line2 == '':
                    while True:
                        if line1 == '':
                            break
                        output_temp.write(line1 + '\n')
                        line1 = cur_batch_file.readline().strip('\n')
                    break
                
                # if both files are not empty, merge
                word1 = eval(line1)[0]
                word2 = eval(line2)[0]
                
                while word1 > word2:
                    output_temp.write(line2 + '\n')
                    line2 = input_temp.readline().strip('\n')
                    if line2 == '':
                        break
                    word2 = eval(line2)[0]
                    
                while word2 > word1:
                    output_temp.write(line1 + '\n')
                    line1 = cur_batch_file.readline().strip('\n')
                    if line1 == '':
                        break
                    word1 = eval(line1)[0]
                
                if (word1 == word2):
                    temp_dict = self.merge_posting(eval(line1)[1], eval(line2)[1])
                    output_temp.write(str((word1, temp_dict)) + '\n')
                    line1 = cur_batch_file.readline().strip('\n')
                    line2 = input_temp.readline().strip('\n')
            
            cur_batch_file.close()
            input_temp.close()
            output_temp.close()
            cur_temp = (cur_temp + 1) % 2
            
        self.remove_merge_temp_files(cur_temp)
               
        
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
            titles += title.text + ','
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
        

    def get_token_posting_locations(self):
        index_file = open('./indexes/index.txt', 'r')
        with open('./indexes/token_posting_locations.json', 'w') as token_posting_location_file:
            token_posting_location_dict = dict()
            
            while True:
                pointer = index_file.tell()
                line = index_file.readline().strip('\n')
                if line == '':
                    break
                tup = eval(line) # tuple of (token, postings)
                token_posting_location_dict[tup[0]] = pointer
            
            json.dump(token_posting_location_dict, token_posting_location_file)
        
        index_file.close()
    
    
    def merge_posting(self, posting1, posting2):
        d = {k : v for k, v in sorted({** posting1, ** posting2}.items())}
        return d
    
    
    def remove_merge_temp_files(self, cur_temp):
        if cur_temp == 0:
            os.remove('./indexes/temp1.txt')
            os.rename('./indexes/temp0.txt', './indexes/index.txt')
        else:
            os.remove('./indexes/temp0.txt')
            os.rename('./indexes/temp1.txt', './indexes/index.txt')


    def clear_merge_temp_files(self):
        with open('./indexes/temp0.txt', 'r+') as temp0, open('./indexes/temp1.txt', 'r+') as temp1:
            temp0.truncate(0)
            temp1.truncate(0)    
                
        
def generate_report():
    with open('report_MS3.txt', 'w') as report_file:
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
    indexer.binary_merge_index_batches('./indexes')
    indexer.get_token_posting_locations()
    generate_report()



            
    
    
    