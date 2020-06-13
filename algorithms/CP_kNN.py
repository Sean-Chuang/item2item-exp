# Condition Probability Knn
from model import Model
import numpy as np
from scipy.sparse import dok_matrix, csc_matrix
from tqdm import tqdm
import time, sys

class CP_kNN(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 
                        'train_raw_file', 'user_idx', 'item_idx', 'behavior_idx']
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
        self.user_idx, self.item_idx,  self.item_idx_reverse = {}, {}, {} 
        self.behavior_idx, self.behavior_idx_reverse = {}, {}
        with open(self.config['user_idx'], 'r') as in_f:
            for line in in_f:
                user, idx = line.strip().split('\t')
                self.user_idx[user] = int(idx)
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
        self.user_item_table = dok_matrix((len(self.user_idx), len(self.item_idx)), dtype=np.float)
        # Build the sparse metric for user-item or user-behavior
        print('Matrix size : ', self.user_item_table.shape)
        with open(self.config['train_raw_file'], 'r') as in_f:
            for line in tqdm(in_f):
                ad_id, item_id, behavior, ts = line.strip().split('\t')
                if item_id:
                    self.user_item_table[self.user_idx[ad_id], self.item_idx[item_id]] += 1.0
        self.user_item_table = self.user_item_table.tocsr() 
        self.item_nonzero_count = np.bincount(self.user_item_table.indices)
        print("Train finished ...")


    def __item_item_score(self, given_idx, candidate_idx, alpha=0.5):
        # Given v: 
        #   sim(u, v) = freq(u & v) / (freq(v) * freq(u)^0.5)
        s_time = time.time()
        freq_u_v = self.user_item_table[:,given_idx].multiply(self.user_item_table[:,candidate_idx]).count_nonzero()
        freq_u, freq_v = self.item_nonzero_count[candidate_idx], self.item_nonzero_count[given_idx]
        score = freq_u_v / (freq_v * (freq_u ** alpha))
        return score


    def __item_item_arr_norm_score(self, given_idx, candidate_idx_arr, alpha=0.5):
        res = [self.__item_item_score(given_idx, candidate_idx, alpha) for candidate_idx in candidate_idx_arr]
        res = np.array(res)
        return  res / np.linalg.norm(res)


    def __item_similarity(self, given_idx, topK):
        u_idx = self.user_item_table[:,given_idx].nonzero()[0]
        candidate_item_idx = np.unique(self.user_item_table[u_idx].nonzero()[1])
        items_score = {}
        for idx in candidate_item_idx:
            items_score[idx] = self.__item_item_score(given_idx, idx)
        return [k for k, v in sorted(items_score.items(), key=lambda item: item[1], reverse=True)]


    def predict(self, last_n_events, topN):
        candidate_set = set()
        last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1]]
        for item_idx in last_n_items:
            candidate = self.__item_similarity(item_idx, topN)
            candidate_set.update(candidate)

        candidate_list = list(candidate_set)
        score_matric = np.zeros((len(last_n_items), len(candidate_list)))
        for i, item_id in enumerate(last_n_items):
            score_matric[i] = self.__item_item_arr_norm_score(item_id, candidate_list)

        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = rank_weight.dot(score_matric).tolist()
        # print(list(zip(candidate_list, final_score)))
        final_items = sorted(zip(candidate_list, final_score), key=lambda x:x[1], reverse=True)
        return [self.item_idx_reverse[item] for item, score in final_items[:topN]]



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10,
        'train_raw_file': '../data/sample.csv',
        'user_idx': '../data/user_idx.csv',
        'item_idx': '../data/item_idx.csv',
        'behavior_idx': '../data/behavior_idx.csv'
    }
    model = CP_kNN(config)
    model.train()
    model.test()
    model.print_metrics()