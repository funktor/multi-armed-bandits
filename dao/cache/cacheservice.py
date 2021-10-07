import json
import flask
from flask import Flask, request, jsonify
import redis

class CacheService(object):
    conn = None
    
    @classmethod
    def get_cache(cls):
        if cls.conn is None:
            cls.conn = redis.Redis(host='redis', port=6379)
            
        return cls.conn
    
    @classmethod
    def insert_consistent_hash_model(cls, key, value):
#         try:
        CacheService.get_cache()
        cls.conn.zadd('consistent_hash_model', {key : value})

        return 1
#         except:
#             return None
        
    @classmethod
    def insert_consistent_hash_user(cls, key, value):
#         try:
        CacheService.get_cache()
        cls.conn.zadd('consistent_hash_user', {key : value})

        return 1
#         except:
#             return None
    
    @classmethod
    def get_all_models(cls):
#         try:
        CacheService.get_cache()
        return cls.conn.zrange('consistent_hash_model', 0, -1, withscores=True)
#         except:
#             return None
        
    @classmethod
    def get_all_users(cls):
#         try:
        CacheService.get_cache()
        return cls.conn.zrange('consistent_hash_user', 0, -1, withscores=True)
#         except:
#             return None
        
        
    @classmethod
    def get_model_scores(cls, key):
#         try:
        CacheService.get_cache()
        return cls.conn.zscore('consistent_hash_model', key)
#         except:
#             return None
        
    @classmethod
    def get_user_scores(cls, key):
#         try:
        CacheService.get_cache()
        return cls.conn.zscore('consistent_hash_user', key)
#         except:
#             return None