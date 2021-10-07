from __future__ import print_function

import json, collections
import flask, uuid
from flask import Flask, request, jsonify
import requests

app = flask.Flask(__name__)

@app.route("/create_test", methods=["POST"])
def create_test():
    data = request.get_json()
    print(data)
    
    if 'test_name' in data:
        h1 = requests.put('http://db:3003/create_events_table', 
                      data=json.dumps({}), 
                      headers={'Content-type': 'application/json'})
        
        if h1.status_code != 201:
            return flask.Response(status=500)
        
        topic = 'create_test'
        event_key = str(uuid.uuid4())
        data['event_key'] = event_key
        data['event_type'] = 'create_test'
        data['status'] = 'QUEUED'

        h1 = requests.put('http://db:3003/insert_event', 
                          data=json.dumps(data), 
                          headers={'Content-type': 'application/json'})

        print(h1.status_code)
        
        if h1.status_code == 201:
            h2 = requests.put('http://queue:3001/insert_queue', 
                            data=json.dumps({'item':json.dumps(data)}), 
                            headers={'Content-type': 'application/json'})
            
            print(h2.status_code)
            
            if h2.status_code == 201:
                return flask.jsonify(data=data)
            else:
                return flask.Response(status=500)
            
        else:
            return flask.Response(status=500)
        
    return flask.Response(status=400)

@app.route("/create_variant", methods=["POST"])
def create_variant():
    data = request.get_json()
    
    if 'test_name' in data and 'host' in data and 'port' in data:
        topic = 'create_variant'
        event_key = str(uuid.uuid4())
        data['event_key'] = event_key
        data['event_type'] = 'create_variant'
        data['status'] = 'QUEUED'
        
        h1 = requests.put('http://db:3003/insert_event', 
                          data=json.dumps(data), 
                          headers={'Content-type': 'application/json'})
        
        print(h1.status_code)

        if h1.status_code == 201:
            h2 = requests.put('http://queue:3001/insert_queue', 
                              data=json.dumps({'item':json.dumps(data)}), 
                              headers={'Content-type': 'application/json'})
            
            print(h2.status_code)
            
            if h2.status_code == 201:
                return flask.jsonify(data=data)
            else:
                return flask.Response(status=500)
            
        else:
            return flask.Response(status=500)
        
    return flask.Response(status=400)

@app.route("/status", methods=["GET"])
def status():
    if "event_key" in request.args:
        event_key = request.args.get("event_key")
        
        r = requests.get('http://db:3003/event_status', 
                         params={'event_key':event_key})

        if r.status_code == 200:
            h = r.json()
            return flask.jsonify(data={'event_key': event_key, 'status':h['data']})
        else:
            return flask.Response(status=500)
        
    return flask.Response(status=400)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')


