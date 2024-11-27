from flask import Flask, jsonify, request 
from querier.querier import Querier
import time
import numpy as np

# creating a Flask app 
app = Flask(__name__) 

q = Querier(10000)

  
@app.route('/search/<querystr>', methods=['GET'])
def get_string(querystr):
    start_time = time.monotonic()
    
    print(len(q.query(querystr)))
    result = np.array(q.query(querystr))
    if len(result)%10 != 0:
        result = np.array(result[:-(len(result)%10)]).reshape(-1, 10).tolist()
        result.append(result[-(len(result)%10):])
    else:
        result = result.reshape(-1, 10).tolist()
    
    
    query_result = jsonify({
        'results': result
        })
    query_result.headers.add('Access-Control-Allow-Origin', '*')
    print(f"Took {(time.monotonic() - start_time):.3f} Seconds\n")
    return query_result, 200
  
  
# driver function 
if __name__ == '__main__':
    app.run(debug = True) 