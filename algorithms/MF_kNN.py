# Condition Probability Knn
from model import Model
import numpy as np
from scipy.sparse import dok_matrix, csc_matrix
from tqdm import tqdm
from implicit.bpr import BayesianPersonalizedRanking
from annoy import AnnoyIndex
import time, sys

class MF_kNN(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 'train_file', 'index_file_file']
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

        self.item_idx = {}


    def train(self):
        b_time = time.time()
        Y = []
        
        with open(train_file, 'r') as in_f:
            for idx, line in tqdm(enumerate(in_f)):
                items_list = line.strip().split()
                Y.append([self.__get_id(item) for item in items_list])
        # construct the sparse matrix
        indptr = np.fromiter(chain((0,), map(len, Y)), int, len(Y) + 1).cumsum()
        indices = np.fromiter(chain.from_iterable(Y), int, indptr[-1])
        data = np.ones_like(indices)
        user_item_table_csr = csr_matrix((data, indices, indptr))
        print('Matrix size : ', user_item_table_csr.shape)
        print("Train finished ... : ", time.time() - b_time)

        # Train MF
        model_name = "bpr"
        self.model = BayesianPersonalizedRanking(num_threads=20)
        print("training model %s", model_name)
        start = time.time()
        self.model.fit(data_csr)
        print("trained model '%s' in %s", model_name, time.time() - start)
        print("calculating top movies")

        # Build Ann
        self.t = AnnoyIndex(int(dim), 'angular')
            
        for idx, vec in tqdm(enumerate(self.model.item_factors)):
            self.t.add_item(idx, vec)

        print("Read file finished ...")
        file_name = self.config['index_file_file']

        self.t.build(30) # 10 trees
        self.t.save(f'{file_name}.ann')

        # self.t.load(f'{file_name}.ann')

        print(f"Train finished ...{time.time() - b_time}")


    def __get_id(self, item):
        if item in self.item_idx:
            _id = self.item_idx[item]
        else:
            _id = len(self.item_idx)
            self.item_idx[item] = _id

        return _id


    def predict(self, last_n_events, topN):
        b_time = time.time()
        item_similar = list()
        candidate_items = set()
        
        last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1] if e.split(':', 1)[1] in self.item_idx]
        
        if len(last_n_items) == 0:
            return []

        for item_idx in last_n_items:
            similar_res = self.__item_topK_similar(item_idx, topN)
            item_similar.append(similar_res)
            candidate_items.update(set(similar_res.keys()))

        candidate_list = list(candidate_items)
        score_matric = np.zeros((len(last_n_items), len(candidate_list)))
        for i, item_id in enumerate(last_n_items):
            score_matric[i] = self.__item_item_arr_norm_score(item_id, candidate_list, item_similar[i])

        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = rank_weight.dot(score_matric).tolist()

        # print(last_n_items, list(zip(candidate_list, final_score)))
        final_items = sorted(zip(candidate_list, final_score), key=lambda x:x[1], reverse=True)
        return [item for item, score in final_items[:topN]]


    def __item_topK_similar(self, given_idx, topK):
        item_idx_arr, score_arr = self.t.get_nns_by_item(given_idx, topK, include_distances=True)
        res = {}
        for idx, score in zip(item_idx_arr, score_arr):
            try:
                item_raw = self.item_idx_reverse[idx]
                if item_raw not in res:
                    # return to cosine score
                    res[item_raw] = 1 - score**2/2
            except:
                pass
            
        return res


    def __item_item_arr_norm_score(self, item, candidate_item_arr, similar_items):
        res = np.zeros(len(candidate_item_arr))
        for _item in similar_items:
            _score = similar_items[_item]
            if _item in candidate_item_arr:
                res[candidate_item_arr.index(_item)] = float(_score)
        return res / np.linalg.norm(res)



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10,
        'train_file': '../data/sample.csv',
        'index_file_file': '../tmp/sample'
    }
    model = MF_kNN(config)
    model.train()
    model.test()
    model.print_metrics()