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
    query = Query(Indexer())
    if request.method == 'POST':
        search_content = request.form['content']
        try:
            if search_content == '':
                pass
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
            print(f"Search Time: {(search_time * 1000):.2f}ms")
            return render_template('index.html', results=results, search_content=search_content)
    else:
        return render_template('index.html') #, results=results, search_content=search_content)
    
if __name__=='__main__':
    app.run(debug=True)