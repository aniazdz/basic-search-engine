from flask import Flask, render_template, request, redirect, url_for
from query_rank_retrieval import Query
from indexer import Indexer
import time
from datetime import datetime
# sourced from https://flask.palletsprojects.com/en/2.2.x/tutorial/
# -- https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3

app = Flask('__name__') 

@app.route('/', methods=['POST', 'GET'])

def index():
    query_log_file = open('query_log.txt', 'a')
    query = Query(Indexer())
    if request.method == 'POST':
        search_content = request.form['content']
        try:
            if search_content == '':
                pass
            query_log_file.write(f"Query: {search_content}\n")
            start_time = time.time()
            query.get_query_tokens(search_content)
            query.ranking_retrieval()
        except:

            return redirect('/')
            # return 'There was a problem processing your query'
        #\n Your query may have been empty or not processed correctly. \n\tPlease reload the page.
        else:
            # print(results)
            results = query.result()
            end_time = time.time()
            search_time = end_time - start_time
            query_log_file.write(f"Search time: {(search_time * 1000):.2f}ms\n")
            for result in results:
                query_log_file.write(f"{result}\n")
            query_log_file.write('\n\n')
            print(f"Search Time: {(search_time * 1000):.2f}ms")
            return render_template('index.html', results=results, search_content=search_content)
    else:
        query_log_file.close()
        return render_template('index.html') #, results=results, search_content=search_content)
    
if __name__=='__main__':
    app.run(debug=True)
    