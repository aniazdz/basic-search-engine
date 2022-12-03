# cs121-search-engine-40
Milestone 1: Index construction

Builds an inverted index using provided HTML files which it uses to to create a token and its corresponding postings. The postings are assigned a weight
with text types such as Titles and subtitles with greater weight than body text. The processed information is then used to generate a report.

## Directions
To initiate the search engine, you need to run app.py, but before this make sure you have these files in the indexes directory: index.txt (the whole inverted index), docid_url_map.json (a dictionary that stores document ids as the keys and their corresponding urls as the values), and token_posting_locations.json (a dictionary that stores tokens as the keys and the positions of the heads of tokens’ postings in index.txt as the values). If not, make sure the DEV dataset is at the root directory and run indexer.py to generate the indexes directory and all files used for querying. It may take 3-10 minutes depending on your computer.
