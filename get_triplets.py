import re, os, pickle
import multiprocessing
from multiprocessing import Process, Manager

def readfile(file, lst):
    movie_id = None
    
    with open(os.path.join('/Users/abhijitmondal/Downloads/Netflix', file)) as f:
        while True:
            line = f.readline()
            if len(line) == 0:
                break
            if re.match('[0-9]+:', line):
                movie_id = line[:-2]
            else:
                d = line.split(',')
                if len(d) >= 2:
                    user_id = d[0]
                    rating = d[1]
                    lst.append((user_id, movie_id, float(rating)))

if __name__ == '__main__':
    manager = Manager()
    triplets = manager.list()

    files = ['combined_data_1.txt', 'combined_data_2.txt', 'combined_data_3.txt', 'combined_data_4.txt']

    procs = []

    for i in range(len(files)):
        p = Process(target=readfile, args=(files[i], triplets))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()
    
    with open('data.pkl', 'wb') as f:
        pickle.dump(list(triplets), f, protocol=pickle.HIGHEST_PROTOCOL)