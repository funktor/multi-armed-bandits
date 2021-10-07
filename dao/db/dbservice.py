import json
import flask
from flask import Flask, request, jsonify
import psycopg2, hashlib

def get_schema_name(test_name):
    return 'test__' + str(hashlib.md5(test_name.encode()).hexdigest())

class DBService(object):
    conn, cur = None, None
    
    @classmethod
    def get_db(cls):
        if cls.conn is None:
            cls.conn = psycopg2.connect(host="postgres", database="postgres", user="postgres", password="root123")
            cls.cur = cls.conn.cursor()
            
        return cls.conn, cls.cur
    
    @classmethod
    def create_events_table(cls):
        try:
            DBService.get_db()
            cls.cur.execute('CREATE TABLE IF NOT EXISTS public.events(event_key text primary key, status text, optional_message text)')
            cls.conn.commit()

            return 1
        except:
            return None
    
    @classmethod
    def insert_event(cls, event_key, status):
        try:
            DBService.get_db()
            cls.cur.execute('INSERT INTO public.events(event_key, status) VALUES (%s, %s) ON CONFLICT (event_key) DO UPDATE SET status=%s', (event_key, status, status))
            cls.conn.commit()

            return 1
        except:
            return None
        
    @classmethod
    def insert_model_config(cls, test_name, model_id, host, port):
        try:
            DBService.get_db()

            schema_name = get_schema_name(test_name)

            cls.cur.execute('INSERT INTO ' + schema_name + '.model_config(model_id, host, port) VALUES (%s, %s, %s) ON CONFLICT (model_id) DO UPDATE SET host=%s, port=%s', (model_id, host, port, host, port))
            cls.conn.commit()

            return 1
        except:
            return None
        
    @classmethod
    def insert_user_assignment(cls, test_name, user_id, model_id):
        try:
            DBService.get_db()

            schema_name = get_schema_name(test_name)

            cls.cur.execute('INSERT INTO ' + schema_name + '.user_assignment(user_id, model_id) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET model_id=%s', (user_id, model_id, model_id))
            cls.conn.commit()

            return 1
        except:
            return None
    
    @classmethod
    def create_test(cls, test_name):
        try:
            DBService.get_db()
            schema_name = get_schema_name(test_name)

            cls.cur.execute('CREATE SCHEMA IF NOT EXISTS global')
            cls.cur.execute('CREATE TABLE IF NOT EXISTS global.tests(schema text primary key, is_active boolean)')
            cls.cur.execute('INSERT INTO global.tests(schema, is_active) VALUES (%s, TRUE) ON CONFLICT DO NOTHING', (schema_name,))

            cls.cur.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
            cls.cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.user_assignment(user_id varchar(10) primary key, model_id bigint)')
            cls.cur.execute('CREATE INDEX IF NOT EXISTS model_index ON ' + schema_name + '.user_assignment(model_id)')
            cls.cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.model_config(model_id bigint primary key, host text, port text)')
            cls.cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.ctr_tracking(model_id bigint primary key, num_displayed bigint, num_clicked bigint)', (schema_name,))

            cls.conn.commit()
            return 1

        except:
            return None
    
    @classmethod
    def get_event_status(cls, event_key):
        try:
            DBService.get_db()
            cls.cur.execute('SELECT status FROM public.events WHERE event_key=%s', (event_key,))
            row = cls.cur.fetchone()
            return row[0]
        except:
            return None

    @classmethod
    def get_url(cls, user_id, test_name):
        try:
            DBService.get_db()
            schema_name = get_schema_name(test_name)

            cls.cur.execute('SELECT b.host, b.port FROM ' + schema_name + '.user_assignment a INNER JOIN ' + schema_name + '.model_config b ON a.model_id=b.model_id WHERE a.user_id=%s', (user_id,))
            row = cls.cur.fetchone()
            return row
        
        except:
            return None
        
    @classmethod
    def get_is_active_test(cls, test_name):
        try:
            DBService.get_db()
            schema_name = get_schema_name(test_name)

            cls.cur.execute('SELECT is_active FROM global.tests WHERE schema=%s', (schema_name,))
            row = cls.cur.fetchone()
            return row[0]
        
        except:
            return None
        
    @classmethod
    def get_all_user_assignments(cls, test_name):
        try:
            DBService.get_db()
            schema_name = get_schema_name(test_name)

            cls.cur.execute('SELECT user_id, model_id FROM ' + schema_name + '.user_assignment')
            return cls.cur.fetchall()
        
        except:
            return None