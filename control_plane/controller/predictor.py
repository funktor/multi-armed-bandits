from __future__ import print_function

import json
import flask, requests
from flask import Flask, request, jsonify

app = flask.Flask(__name__)

@app.route("/recommendations", methods=["GET"])
def recommendations():
    user_id = request.args.get("user_id")
    test_name = request.args.get("test_name")
  
    r = requests.get('http://db:3003/assigned_url', params={'user_id':user_id, 'test_name':test_name})
    
    if r.status_code == 200:
        data = r.json()['data']
        print(data)
        host = data['host']
        port = data['port']
        host = 'model-' + str(port)
        
        r = requests.get('http://' + host + ':' + str(port) + '/recommendations', params={'user_id':user_id})
        
        if r.status_code == 200:
            data = r.json()['data']
            return flask.jsonify(data=data)
    
    return flask.Response(status=500)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')



