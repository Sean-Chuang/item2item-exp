# Condition Probability Knn
from model import Model
import numpy as np
from collections import defaultdict
from scipy.sparse import csr_matrix
from tqdm import tqdm
import time, sys
from itertools import chain

class CP_kNN(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 
                        'train_file', 'item_idx', 'behavior_idx']
        self.config = config
        miss = set()
        for item in self.requirement:
            if item not in self.config:
                miss.add(item)
        if len(miss) > 0:
            raise Exception(f"Miss the key : {miss}")

        Model.__init__(self, 
                self.config['test_file'], 
                self.config['lastN'],
                self.config['topN']
            )

        # read user_idx & item_idx & behavior_idx
        self.item_idx,  self.item_idx_reverse = {}, {}
        self.behavior_idx, self.behavior_idx_reverse = {}, {}
        with open(self.config['item_idx'], 'r') as in_f:
            for line in in_f:
                item, idx = line.strip().split('\t')
                self.item_idx[item] = int(idx) 
                self.item_idx_reverse[int(idx)] = item 
        with open(self.config['behavior_idx'], 'r') as in_f:
            for line in in_f:
                behavior, idx = line.strip().split('\t')
                self.behavior_idx[behavior] = int(idx)
                self.behavior_idx_reverse[int(idx)] = behavior 

    def train(self):
        # self.user_item_matrix = csr_matrix()
        Y = []
        with open(self.config['train_file'], 'r') as in_f:
            for idx, line in tqdm(enumerate(in_f)):
                item_ids = line.strip().split()
                Y.append([self.item_idx[_id.split(':', 1)[1]] for _id in item_ids])
        # construct the sparse matrix
        indptr = np.fromiter(chain((0,), map(len, Y)), int, len(Y) + 1).cumsum()
        indices = np.fromiter(chain.from_iterable(Y), int, indptr[-1])
        data = np.ones_like(indices)
        self.user_item_table = csr_matrix((data, indices, indptr), (len(Y), len(self.item_idx)))
        self.item_nonzero_count = np.bincount(self.user_item_table.indices)
        print('Matrix size : ', self.user_item_table.shape)
        print("Train finished ...")


    def __item_item_score(self, given_idx, candidate_idx, alpha=0.5):
        # Given v: 
        #   sim(u, v) = freq(u & v) / (freq(v) * freq(u)^0.5)
        s_time = time.time()
        # freq_u_v = self.user_item_table[:,given_idx].multiply(self.user_item_table[:,candidate_idx]).count_nonzero()
        freq_u_v = len(self.user_item_table[:,given_idx].nonzero()[0] & self.user_item_table[:,candidate_idx].nonzero()[0])
        freq_u, freq_v = self.item_nonzero_count[candidate_idx], self.item_nonzero_count[given_idx]
        score = freq_u_v / (freq_v * (freq_u ** alpha))
        print(time.time() - s_time)
        return score


    def __item_similarity_score(self, given_idx, user_idx, candidate_idx_arr, alpha=0.5):
        """
        Given v: 
          sim(u, v) = freq(u & v) / (freq(v) * freq(u)^0.5)
        """
        if user_idx is None:
            tmp_matrix = self.user_item_table[:,candidate_idx_arr]
        else:    
            tmp_matrix = self.user_item_table[user_idx][:,candidate_idx_arr]
        
            
        tmp_matrix_nonzero_count = np.bincount(tmp_matrix.indices)
        items_score = {}
        for idx, item_idx in enumerate(candidate_idx_arr):
            freq_u_v = tmp_matrix_nonzero_count[idx]
            freq_u, freq_v = self.item_nonzero_count[item_idx], self.item_nonzero_count[given_idx]
            items_score[item_idx] = freq_u_v / (freq_v * (freq_u ** alpha))
        return items_score


    def __item_item_arr_norm_score(self, given_idx, candidate_idx_arr, alpha=0.5):
        items_score = self.__item_similarity_score(given_idx, None, candidate_idx_arr)
        res = [items_score[item_idx] for item_idx in candidate_idx_arr]
        res = np.array(res)
        return  res / np.linalg.norm(res)


    def __item_similarity(self, given_idx, topK):
        s_time = time.time()
        u_idx = self.user_item_table[:,given_idx].nonzero()[0]
        candidate_item_idx = np.unique(self.user_item_table[u_idx].nonzero()[1])
        items_score = self.__item_similarity_score(given_idx, u_idx, candidate_item_idx)
        # print(f'#user:{len(u_idx)}, #candidate_item:{len(candidate_item_idx)}')
        if len(candidate_item_idx) > 0:
            print('[time|similar_k]', time.time() - s_time)
        return [k for k, v in sorted(items_score.items(), key=lambda item: item[1], reverse=True)]


    def predict(self, last_n_events, topN):
        candidate_set = set()
        last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1]]
        for item_idx in last_n_items:
            candidate = self.__item_similarity(item_idx, topN)
            print(item_idx, self.item_idx_reverse[item_idx], candidate)
            candidate_set.update(candidate)

        candidate_list = list(candidate_set)
        score_matric = np.zeros((len(last_n_items), len(candidate_list)))
        for i, item_id in enumerate(last_n_items):
            score_matric[i] = self.__item_item_arr_norm_score(item_id, candidate_list)
        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = rank_weight.dot(score_matric).tolist()
        # print(last_n_items, list(zip(candidate_list, final_score)))
        final_items = sorted(zip(candidate_list, final_score), key=lambda x:x[1], reverse=True)
        return [self.item_idx_reverse[item] for item, score in final_items[:topN]]



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10,
        'train_file': '../tr_data.csv',
        'item_idx': '../data/item_idx.csv',
        'behavior_idx': '../data/behavior_idx.csv'
    }
    model = CP_kNN(config)
    model.train()
    model.test()
    model.print_metrics()