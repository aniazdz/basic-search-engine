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
        self.path_list = []
        self.doc_id = 1
        self.file_threshold = 2000
    
    
    def get_file_list(self):
        path_set = set()
        path_list = Path(self.directory).rglob('*.json')
        for path in path_list:
            path = str(path)
            if "#" in path:
                path = path[:path.index("#")]
            path_set.add(path)
        self.path_list = list(path_set)
        
        
    def build_index(self):
        if not self.path_list:
            self.get_file_list()
            
        visited_site = set()
        for file in self.path_list:
            with open(file, 'r') as json_file:
                webpage_json = json.load(json_file)
            url, text, titles, headings, bolds = self.process_file_weights(webpage_json)
            
            if url not in visited_site:
                
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
                visited_site.add(url)
                               
                        
            
            
    
    def process_file_weights(self, webpage_json):
        content = webpage_json["content"]
        soup = BeautifulSoup(content, "html.parser")
        text = soup.get_text()
        titles = []
        headings = []
        bolds = []
        
        for title in soup.find_all('title'):
            titles.append(title)
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            headings.append(heading)
        for bold in soup.find_all(['b', 'strong']):
            bolds.append(bold)
        
        return webpage_json["url"], text, titles, headings, bolds
        
        
    
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
        
        
if __name__ == '__main__':
    index = Indexer()
                
    
    
    