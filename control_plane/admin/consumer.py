import json, requests, mmh3, pickle, time
import random

def random_assignment(test_name, host, port):
#     try:
    model_id = mmh3.hash(host+port, signed=False)

    h = requests.put('http://db:3003/insert_model_config', 
                     data=json.dumps({'test_name':test_name, 'model_id':model_id, 'host':host, 'port':port}), 
                     headers={'Content-type': 'application/json'})

    if h.status_code != 201:
        return None

    h = requests.get('http://db:3003/is_active_test', params={'test_name':test_name})

    if h.status_code == 200:
        row = h.json()
        row = row['data']
        print(row)

        h = requests.get('http://db:3003/model_assignments', params={'test_name':test_name})

        if h.status_code == 500 or len(h.json()['data']) == 0:
            with open('sampled_data.pkl', 'rb') as f:
                sampled_data = pickle.load(f)

            user_ids = [x for x, y, z in sampled_data]

            for i in range(len(user_ids)):
                requests.put('http://db:3003/insert_user_assignment', 
                             data=json.dumps({'test_name':test_name, 'model_id':model_id, 'user_id':user_ids[i]}), 
                             headers={'Content-type': 'application/json'})

        else:
            rows = h.json()['data']

            random.shuffle(rows)
            model_ids = list(set([row[1] for row in rows])) + [model_id]

            pairs = []
            for i in range(len(rows)):
                k = i % len(model_ids)
                pairs.append((rows[i][0], model_ids[k]))

            print(pairs)
            print(len(pairs))

            for x, y in pairs:
                requests.put('http://db:3003/insert_user_assignment', 
                             data=json.dumps({'test_name':test_name, 'model_id':y, 'user_id':x}), 
                             headers={'Content-type': 'application/json'})

    return 1
    
#     except:
#         return None

def consistent_hash_assignment(test_name, host, port):
    try:
        model_id = mmh3.hash(host+port, signed=False)

        h = requests.put('http://db:3003/insert_model_config', 
                         data=json.dumps({'test_name':test_name, 'model_id':model_id, 'host':host, 'port':port}), 
                         headers={'Content-type': 'application/json'})

        if h.status_code != 201:
            return None

        model_vids = []

        for i in range(500):
            g = host + port + str(i)
            mid = mmh3.hash(g, signed=False)

            h1 = requests.get('http://cache:3002/get_model_scores', params={'key':str(model_id)+':'+str(mid)})
            h2 = requests.get('http://cache:3002/get_user_scores', params={'key':str(model_id)+':'+str(mid)})

            b1 = h1.json() if h1.status_code == 200 else None
            b2 = h2.json() if h2.status_code == 200 else None

            while b1 is not None or b2 is not None:
                mid = mmh3.hash(g+str(time.time()), signed=False)

                h1 = requests.get('http://cache:3002/get_model_scores', params={'key':str(model_id)+':'+str(mid)})
                h2 = requests.get('http://cache:3002/get_user_scores', params={'key':str(model_id)+':'+str(mid)})

                b1 = h1.json() if h1.status_code == 200 else None
                b2 = h2.json() if h2.status_code == 200 else None

            model_vids.append(mid)

        print(model_vids[:10])


        h = requests.get('http://db:3003/is_active_test', params={'test_name':test_name})

        if h.status_code == 200:
            row = h.json()
            row = row['data']
            print(row)

            h = requests.get('http://db:3003/model_assignments', params={'test_name':test_name})

            if h.status_code == 500 or len(h.json()['data']) == 0:
                with open('sampled_data.pkl', 'rb') as f:
                    sampled_data = pickle.load(f)

                user_ids = [x for x, y, z in sampled_data]

                for i in range(len(user_ids)):
                    requests.put('http://cache:3002/insert_consistent_hash_user', 
                                 data=json.dumps({'key':user_ids[i], 'val':mmh3.hash(user_ids[i], signed=False)}), 
                                 headers={'Content-type': 'application/json'})

                    requests.put('http://db:3003/insert_user_assignment', 
                                 data=json.dumps({'test_name':test_name, 'model_id':model_id, 'user_id':user_ids[i]}), 
                                 headers={'Content-type': 'application/json'})

            else:
                h1 = requests.get('http://cache:3002/get_all_models')
                h2 = requests.get('http://cache:3002/get_all_users')

                mhashes = h1.json()['data'] if h1.status_code == 200 else []
                uhashes = h2.json()['data'] if h2.status_code == 200 else []

                print(mhashes)
                print(uhashes)

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
                        pairs.append((uhashes[q][0] , model_vid))
                        q = (q-1) % len(uhashes)

                print(pairs)

                for x, y in pairs:
                    requests.put('http://cache:3002/insert_consistent_hash_user', 
                                 data=json.dumps({'key':x, 'val':mmh3.hash(x, signed=False)}), 
                                 headers={'Content-type': 'application/json'})

                    requests.put('http://db:3003/insert_user_assignment', 
                                 data=json.dumps({'test_name':test_name, 'model_id':model_id, 'user_id':x}), 
                                 headers={'Content-type': 'application/json'})

        for model_vid in model_vids:
            requests.put('http://cache:3002/insert_consistent_hash_model', 
                                 data=json.dumps({'key':str(model_id)+':'+str(model_vid), 'val':model_vid}), 
                                 headers={'Content-type': 'application/json'})

        return 1
    
    except:
        return None
    
if __name__ == '__main__':
    while True:
        r = requests.get('http://queue:3001/get_queue')
        
        if r.status_code == 200:
            data = r.json()
            data = json.loads(data['data'])
            print(data)
            
            data['status'] = 'PROCESSING'
            h = requests.put('http://db:3003/insert_event', data=json.dumps(data), headers={'Content-type': 'application/json'})
            print(h.status_code)
            
            if h.status_code == 201:
                if data['event_type'] == 'create_test':
                    h = requests.put('http://db:3003/create_test', data=json.dumps(data), headers={'Content-type': 'application/json'})
                    g = 1 if h.status_code == 201 else None
                else:
                    g = random_assignment(data['test_name'], data['host'], data['port'])

                if g is not None:
                    data['status'] = 'COMPLETED'
                else:
                    data['status'] = 'FAILED'
                
                requests.put('http://db:3003/insert_event', data=json.dumps(data), headers={'Content-type': 'application/json'})