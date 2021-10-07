from __future__ import print_function

import json
import flask
from flask import Flask, request, jsonify
from cacheservice import CacheService

app = flask.Flask(__name__)

@app.route("/insert_consistent_hash_model", methods=["PUT"])
def insert_consistent_hash_model():
    data = request.get_json()
    
    if 'key' in data and 'val' in data:
        resp = CacheService.insert_consistent_hash_model(data['key'], data['val'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/insert_consistent_hash_user", methods=["PUT"])
def insert_consistent_hash_user():
    data = request.get_json()
    
    if 'key' in data and 'val' in data:
        resp = CacheService.insert_consistent_hash_user(data['key'], data['val'])
        if resp:
            return flask.Response(status=201)
        else:
            return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/get_all_models", methods=["GET"])
def get_all_models():
    data = CacheService.get_all_models()

    if data:
        data = [(x.decode("utf-8"), y) for x, y in data]
        return flask.jsonify(data=data)
    else:
        return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/get_all_users", methods=["GET"])
def get_all_users():
    data = CacheService.get_all_users()

    if data:
        data = [(x.decode("utf-8"), y) for x, y in data]
        return flask.jsonify(data=data)
    else:
        return flask.Response(status=500)

    return flask.Response(status=400)

@app.route("/get_model_scores", methods=["GET"])
def get_model_scores():
    if 'key' in request.args:
        data = CacheService.get_model_scores(request.args.get('key'))

        if data:
            return flask.jsonify(data=data)
        else:
            return flask.Response(status=500)

        return flask.Response(status=400)

@app.route("/get_user_scores", methods=["GET"])
def get_user_scores():
    if 'key' in request.args:
        data = CacheService.get_user_scores(request.args.get('key'))

        if data:
            return flask.jsonify(data=data)
        else:
            return flask.Response(status=500)

        return flask.Response(status=400)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')



