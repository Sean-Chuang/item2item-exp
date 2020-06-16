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
        b_time = time.time()
        Y = []
        with open(self.config['train_file'], 'r') as in_f:
            for idx, line in tqdm(enumerate(in_f)):
                item_ids = line.strip().split()
                Y.append([self.item_idx[_id.split(':', 1)[1]] for _id in item_ids])
        # construct the sparse matrix
        indptr = np.fromiter(chain((0,), map(len, Y)), int, len(Y) + 1).cumsum()
        indices = np.fromiter(chain.from_iterable(Y), int, indptr[-1])
        data = np.ones_like(indices)
        self.user_item_table_csr = csr_matrix((data, indices, indptr))
        self.user_item_table_lil = self.user_item_table_csr.tolil()
        self.user_item_table_csc = self.user_item_table_csr.tocsc()
        self.item_nonzero_count = self.user_item_table_csr.getnnz(axis=0)
        self.max_column_idx = self.user_item_table_csr.shape[1]
        print('Matrix size : ', self.user_item_table_csr.shape)
        print("Train finished ... : ", time.time() - b_time)


    def predict(self, last_n_events, topN):
        candidate_set = set()
        last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1]]
        for item_idx in last_n_items:
            if item_idx >= self.max_column_idx:
                continue
            _similar_res = self.__item_similarity(item_idx, topN)
            print(_similar_res)
            candidate_set.update([_item for _item, score in _similar_res])

        candidate_set -= set(last_n_items)
        print('candidate_set', candidate_set)
        if len(candidate_set) == 0:
            return []

        candidate_list = list(candidate_set)
        score_matric = np.zeros((len(last_n_items), len(candidate_list)))
        for i, item_id in enumerate(last_n_items):
            score_matric[i] = self.__item_item_arr_norm_score(item_id, candidate_list)
        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = rank_weight.dot(score_matric).tolist()
        # print(last_n_items, list(zip(candidate_list, final_score)))
        final_items = sorted(zip(candidate_list, final_score), key=lambda x:x[1], reverse=True)
        return [self.item_idx_reverse[item] for item, score in final_items[:topN]]


    def __item_similarity(self, given_idx, topK, alpha=0.5):
        b_time = time.time()
        # print('given_idx', given_idx)
        u_idx = self.user_item_table_csc[:,given_idx].nonzero()[0]
        candidate_item_idx = np.unique(self.user_item_table_lil[u_idx].nonzero()[1])
        # print(f'u_idx : {len(u_idx)}, candidate_item_idx: {len(candidate_item_idx)}')
        tmp_matrix = self.user_item_table_lil[u_idx][:,candidate_item_idx]
        tmp_nonzero_count = tmp_matrix.getnnz(axis=0)
        _score = tmp_nonzero_count / (self.item_nonzero_count[given_idx] * self.item_nonzero_count[candidate_item_idx]**alpha)
        res = sorted(zip(candidate_item_idx, _score), key=lambda x: x[1], reverse=True)
        # print(given_idx, res)
        # print('Time : ', time.time() - b_time)
        return res


    def __item_item_arr_norm_score(self, given_idx, candidate_idx_arr, alpha=0.5):
        b_time = time.time()
        # print(given_idx, candidate_idx_arr)
        u_idx = self.user_item_table_csc[:,given_idx].nonzero()[0]
        tmp_matrix = self.user_item_table_lil[u_idx][:,candidate_idx_arr]
        tmp_nonzero_count = tmp_matrix.getnnz(axis=0)
        res = tmp_nonzero_count / (self.item_nonzero_count[given_idx] * self.item_nonzero_count[candidate_idx_arr]**alpha)
        # print('Time : ', time.time() - b_time)
        return  res / np.linalg.norm(res)



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