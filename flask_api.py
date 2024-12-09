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
    
    result = q.query(querystr)
    print(result)
    if len(result) > 10:
        if len(result)%10 != 0:
            result = np.array(result[:-(len(result)%10)]).reshape(-1, 10).tolist()
            result.append(result[-(len(result)%10):])
        else:
            result = np.array(result).reshape(-1, 10).tolist()
    else:
        result = [result]
    print(type(result))
    
    
    query_result = jsonify({
        'results': result,
        'timeTaken': f'{((time.monotonic() - start_time) * 1000):.3f}'
        })
    query_result.headers.add('Access-Control-Allow-Origin', '*')
    print(f"Took {((time.monotonic() - start_time)/1000):.3f} Seconds\n")
    return query_result, 200
  
  
# driver function 
if __name__ == '__main__':
    app.run(debug = True) 