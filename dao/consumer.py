import psycopg2
import json
import hashlib, uuid
import mmh3, redis, pickle, time

def get_postgres_connection():
    conn = psycopg2.connect(host="postgres", database="postgres", user="postgres", password="root123")
    cur = conn.cursor()
    
    return conn, cur

def get_redis_connection():
    return redis.Redis(host='redis', port=6379)

def create_test(test_name, conn, cur):
    try:
        schema_name = 'test__' + str(hashlib.md5(test_name.encode()).hexdigest())

        cur.execute('CREATE SCHEMA IF NOT EXISTS global')
        cur.execute('CREATE TABLE IF NOT EXISTS global.tests(schema text primary key, is_active boolean)')
        cur.execute('INSERT INTO global.tests(schema, is_active) VALUES (%s, TRUE) ON CONFLICT DO NOTHING', (schema_name,))

        cur.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
        cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.user_assignment(user_id varchar(10) primary key, model_id bigint)')
        cur.execute('CREATE INDEX IF NOT EXISTS model_index ON ' + schema_name + '.user_assignment(model_id)')
        cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.model_config(model_id bigint primary key, model_endpoint text)')
        cur.execute('CREATE TABLE IF NOT EXISTS ' + schema_name + '.ctr_tracking(model_id bigint primary key, num_displayed bigint, num_clicked bigint)', (schema_name,))

        conn.commit()
        return 1
    
    except:
        return None

def add_model_random_assignment(test_name, model_endpoint, conn, cur, redis_conn):
    try:
        schema_name = 'test__' + str(hashlib.md5(test_name.encode()).hexdigest())

        model_id = mmh3.hash(model_endpoint, signed=False)
        cur.execute('INSERT INTO ' + schema_name + '.model_config(model_id, model_endpoint) VALUES (%s, %s) ON CONFLICT (model_id) DO UPDATE SET model_endpoint=%s', (model_id, model_endpoint, model_endpoint))
        conn.commit()

        model_vids = []

        for i in range(100):
            g = model_endpoint + str(i)
            mid = mmh3.hash(g, signed=False)

            b1 = redis_conn.zscore('consistent_hash_model', str(model_id)+':'+str(mid))
            b2 = redis_conn.zscore('consistent_hash_user', str(model_id)+':'+str(mid))

            while b1 is not None or b2 is not None:
                mid = mmh3.hash(g+str(time.time()), signed=False)

                b1 = redis_conn.zscore('consistent_hash_model', str(model_id)+':'+str(mid))
                b2 = redis_conn.zscore('consistent_hash_user', str(model_id)+':'+str(mid))

            model_vids.append(mid)

        cur.execute('SELECT is_active FROM global.tests WHERE schema=%s', (schema_name,))
        row = cur.fetchone()

        if len(row) > 0 and row[0]:
            cur.execute('SELECT user_id, model_id FROM ' + schema_name + '.user_assignment')
            rows = cur.fetchall()

            if len(rows) == 0:
                with open('sampled_data.pkl', 'rb') as f:
                    sampled_data = pickle.load(f)

                user_ids = [x for x, y, z in sampled_data]

                for i in range(len(user_ids)):
                    redis_conn.zadd('consistent_hash_user', {user_ids[i] : mmh3.hash(user_ids[i], signed=False)})
                    cur.execute('INSERT INTO ' + schema_name + '.user_assignment(user_id, model_id) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET model_id=%s', (user_ids[i], model_id, model_id))

                conn.commit()

            else:
                mhashes = redis_conn.zrange('consistent_hash_model', 0, -1, withscores=True)
                uhashes = redis_conn.zrange('consistent_hash_user', 0, -1, withscores=True)

                pairs = []

                for model_vid in model_vids:
                    left, right = 0, len(mhashes)-1
                    p = len(mhashes)-1

                    while left <= right:
                        mid = int((left+right)/2)

                        if mhashes[mid][1] < model_vid:
                            p = mid
                            left = mid+1
                        else:
                            right = mid-1


                    left, right = 0, len(uhashes)-1
                    q = len(uhashes)-1

                    while left <= right:
                        mid = int((left+right)/2)

                        if uhashes[mid][1] < model_vid:
                            q = mid
                            left = mid+1
                        else:
                            right = mid-1

                    while uhashes[q][1] > mhashes[p][1]:
                        pairs.append((uhashes[q][0].decode("utf-8") , model_vid))
                        q = (q-1) % len(uhashes)

                for x, y in pairs:
                    redis_conn.zadd('consistent_hash_user', {x : mmh3.hash(x, signed=False)})
                    cur.execute('INSERT INTO ' + schema_name + '.user_assignment(user_id, model_id) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET model_id=%s', (x, model_id, model_id))

                conn.commit()

        for model_vid in model_vids:
            redis_conn.zadd('consistent_hash_model', {str(model_id)+':'+str(model_vid) : model_vid})

        return 1
    
    except:
        return None
    
if __name__ == '__main__':
    conn, cur = get_postgres_connection()
    redis_conn = get_redis_connection()
    key = 'redis_queue'
    
    while True:
        data = redis_conn.blpop(key, timeout=10)
        
        if data:
            data = json.loads(data[1])
            print(data)

            cur.execute('INSERT INTO public.events(event_key, status) VALUES (%s, \'PROCESSING\') ON CONFLICT (event_key) DO UPDATE SET status=\'PROCESSING\'', (data['event_key'],))
            conn.commit()

            if data['event_type'] == 'create_test':
                g = create_test(data['test_name'], conn, cur)
            else:
                g = add_model_random_assignment(data['test_name'], data['endpoint'], conn, cur, redis_conn)

            if g is not None:
                cur.execute('INSERT INTO public.events(event_key, status) VALUES (%s, \'COMPLETED\') ON CONFLICT (event_key) DO UPDATE SET status=\'COMPLETED\'', (data['event_key'],))
            else:
                cur.execute('INSERT INTO public.events(event_key, status) VALUES (%s, \'FAILED\') ON CONFLICT (event_key) DO UPDATE SET status=\'FAILED\'', (data['event_key'],))

            conn.commit()