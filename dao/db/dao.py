from __future__ import print_function

import json
import flask
from flask import Flask, request, jsonify
from dbservice import DBService

app = flask.Flask(__name__)

@app.route("/create_events_table", methods=["PUT"])
def create_events_table():
    resp = DBService.create_events_table()
    if resp:
        return flask.Response(status=201)
    else:
        return flask.Response(status=500)

@app.route("/insert_event", methods=["PUT"])
def insert_event():
    data = request.get_json()
    
    if 'event_key' in data and 'status' in data:
        resp = DBService.insert_event(data['event_key'], data['status'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/insert_model_config", methods=["PUT"])
def insert_model_config():
    data = request.get_json()
    
    if 'test_name' in data and 'model_id' in data and 'host' in data and 'port' in data:
        resp = DBService.insert_model_config(data['test_name'], data['model_id'], data['host'], data['port'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/insert_user_assignment", methods=["PUT"])
def insert_user_assignment():
    data = request.get_json()
    
    if 'test_name' in data and 'model_id' in data and 'user_id' in data:
        resp = DBService.insert_user_assignment(data['test_name'], data['user_id'], data['model_id'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/create_test", methods=["PUT"])
def create_test():
    data = request.get_json()
    
    if 'test_name' in data:
        resp = DBService.create_test(data['test_name'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/event_status", methods=["GET"])
def event_status():
    if 'event_key' in request.args:
        data = DBService.get_event_status(request.args.get("event_key"))
        
        if data:
            return flask.jsonify(data=data)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/is_active_test", methods=["GET"])
def is_active_test():
    if 'test_name' in request.args:
        data = DBService.get_is_active_test(request.args.get("test_name"))
        
        if data:
            return flask.jsonify(data=data)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/assigned_url", methods=["GET"])
def assigned_url():
    if 'user_id' in request.args and 'test_name' in request.args:
        data = DBService.get_url(request.args.get('user_id'), request.args.get('test_name'))
        
        if data:
            return flask.jsonify(data={'host':data[0], 'port':data[1]})
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/model_assignments", methods=["GET"])
def model_assignments():
    if 'test_name' in request.args:
        data = DBService.get_all_user_assignments(request.args.get('test_name'))
        
        if data:
            return flask.jsonify(data=data)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')



