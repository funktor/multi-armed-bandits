import numpy as np
import random, heapq

def loss(data, user_ids_inv, movie_ids_inv, mu, p, q, bu, bm, beta=0.02):
    e = 0
    for u, m, r in data:
        i = user_ids_inv[u]
        j = movie_ids_inv[m]
        reg = beta/2.0*(np.sum(p[i,:]**2)+np.sum(q[:,j]**2+bu[i]**2+bm[j]**2))
        e += (r-mu-bu[i]-bm[j]-np.dot(p[i,:],q[:,j]))**2+reg
        
    return e

class MatrixFactorization:
    def __init__(self, latent_factor_size=50):
        self.latent_factor_size = latent_factor_size
        
        self.mu = 0.0
        self.p, self.q, self.bu, self.bm = None, None, None, None
    
    def train(self, data, alpha=0.0002, beta=0.02, epochs=100):
        self.user_ids = sorted(set([x for x, _, _ in data]))
        self.user_ids_set = set(self.user_ids)
        self.user_ids_inv = {self.user_ids[i]:i for i in range(len(self.user_ids))}

        self.movie_ids = sorted(set([y for _, y, _ in data]))
        self.movie_ids_set = set(self.movie_ids)
        self.movie_ids_inv = {self.movie_ids[i]:i for i in range(len(self.movie_ids))}
        
        self.p = np.random.rand(len(self.user_ids), self.latent_factor_size)
        self.q = np.random.rand(self.latent_factor_size, len(self.movie_ids))
        self.bu = np.random.rand(len(self.user_ids))
        self.bm = np.random.rand(len(self.movie_ids))
        
        self.mu = np.mean([z for x, y, z in data])
    
        for x in range(epochs):
            if len(data) > 10000:
                g = random.sample(data, 10000)
            else:
                g = data

            for u, m, r in g:
                i = self.user_ids_inv[u]
                j = self.movie_ids_inv[m]
                e = r-self.mu-self.bu[i]-self.bm[j]-np.dot(self.p[i,:],self.q[:,j])

                for k in range(self.latent_factor_size):
                    self.p[i][k] = self.p[i][k]+alpha*(2*e*self.q[k][j]-beta*self.p[i][k])
                    self.q[k][j] = self.q[k][j]+alpha*(2*e*self.p[i][k]-beta*self.q[k][j])

                self.bu[i] = self.bu[i]+alpha*(2*e-beta*self.bu[i])
                self.bm[j] = self.bm[j]+alpha*(2*e-beta*self.bm[j])

            if x % 10 == 0:
                l = loss(data, self.user_ids_inv, self.movie_ids_inv, self.mu, self.p, self.q, self.bu, self.bm, beta)
                print(x, l)
    
    def get_recommendations(self, user_id, num_preds=10):
        if user_id in self.user_ids_set:
            i = self.user_ids_inv[user_id]

            heap = []

            for m in self.movie_ids:
                j = self.movie_ids_inv[m]
                rpred = self.mu+self.bu[i]+self.bm[j]+np.dot(self.p[i,:],self.q[:,j])

                if len(heap) < num_preds:
                    heapq.heappush(heap, (rpred, j))
                else:
                    if rpred > heap[0][0]:
                        heapq.heappop(heap)
                        heapq.heappush(heap, (rpred, j))

            return [self.movie_ids[y] for x, y in heap]
        
        return []
        