from __future__ import print_function

import json
import flask
from flask import Flask, request, jsonify
from queueingservice import QueueingService

app = flask.Flask(__name__)

@app.route("/insert_queue", methods=["PUT"])
def insert_queue():
    data = request.get_json()
    print(data)
    
    if 'item' in data:
        resp = QueueingService.insert(data['item'])
        
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/get_queue", methods=["GET"])
def get_queue():
    data = QueueingService.get()

    if data:
        return flask.jsonify(data=data)
    else:
        return flask.Response(status=500)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')



