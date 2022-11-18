from flask import Flask, render_template, request, redirect, url_for
import query_rank_retrieval
import indexer

# sourced from https://flask.palletsprojects.com/en/2.2.x/tutorial/
# -- https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3

app = Flask('__name__')

@app.route('/', methods=['POST', 'GET'])

def index():
    query = query_rank_retrieval.Query(indexer.Indexer())
    # global results, search_content
    # results = []
    # search_content = ''
    if request.method == 'POST':
        search_content = request.form['content']
        # print(search_content)
        try:
            if search_content == '':
                pass
            query.get_query_tokens(search_content)
            query.ranking_retrieval()
        except:

            return redirect('/')
            # return 'There was a problem processing your query'
        #\n Your query may have been empty or not processed correctly. \n\tPlease reload the page.
        else:
            # print(results)
            results = query.result()
            return render_template('index.html', results=results, search_content=search_content)
    else:
        # print(results, 'ahhhhh')
        return render_template('index.html') #, results=results, search_content=search_content)

    
if __name__=='__main__':
    app.run(debug=True)