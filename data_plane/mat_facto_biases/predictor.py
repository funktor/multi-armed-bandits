from __future__ import print_function

import json
import pickle
import flask
from flask import Flask, request, jsonify

class RecommendationService(object):
    predictor = None

    @classmethod
    def get_model(cls):
        if cls.predictor == None:
            with open('facto.pkl', 'rb') as f:
                cls.predictor = pickle.load(f)
                
        return cls.predictor

    @classmethod
    def predict(cls, user_id):
        RecommendationService.get_model()
        
        try:
            return cls.predictor.get_recommendations(user_id)
        except:
            return None

app = flask.Flask(__name__)

@app.route("/recommendations", methods=["GET"])
def recommendations():
    predictions = RecommendationService.predict(str(request.args.get("user_id")))
    
    if predictions:
        return flask.jsonify(data=predictions)
    
    return flask.Response(status=500)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')


