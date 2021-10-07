import json
import flask
from flask import Flask, request, jsonify
import redis

class QueueingService(object):
    queue, key = None, None
    
    @classmethod
    def get_queue(cls):
        if cls.queue is None:
            cls.queue = redis.Redis(host='redisq', port=6379)
            cls.key = 'redis_queue'
            
        return cls.queue
    
    @classmethod
    def insert(cls, data):
#         try:
        QueueingService.get_queue()
        print(data, cls.key)
        cls.queue.rpush(cls.key, data)
        return 1
#         except:
#             return None
        
    @classmethod
    def get(cls):
#         try:
        QueueingService.get_queue()
        out = cls.queue.blpop(cls.key, timeout=10)
        if out:
            return out[1].decode("utf-8")
        else:
            return None
#         except:
#             return None