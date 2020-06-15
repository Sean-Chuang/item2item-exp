from model import Model
import numpy as np
from annoy import AnnoyIndex
import time

class W2V_ANN(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN',
                            'type', 'item_vec_file', 'index_file_file']
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
        self.type = config['type']  # behavior / item


    def train(self):
        b_time = time.time()
        self.item_idx = {}
        self.item_idx_reverse = {}

        with open(self.config['item_vec_file'], 'r') as in_f:
            num_items, dim = in_f.readline().strip().split()
            print(f'Num of items : {num_items}, dim : {dim}')
            self.t = AnnoyIndex(int(dim), 'angular')
            
            for idx, line in enumerate(in_f):
                tmp = line.split()
                self.item_idx[tmp[0]] = idx
                self.item_idx_reverse[idx] = tmp[0]
                self.t.add_item(idx, list(map(float, tmp[1:])))
        print("Read file finished ...")

        self.t.build(100) # 10 trees
        file_name = self.config['index_file_file']
        self.t.save(f'{file_name}.ann')
        print(f"Train finished ...{time.time() - b_time}")
 

    def predict(self, last_n_events, topN):
        b_time = time.time()
        candidate_set = set()
        if self.type == 'item':
            last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1]]
        else:
            last_n_items = [self.item_idx[e] for e in last_n_events[::-1] if e in self.item_idx]
        
        if len(last_n_items) == 0:
            return []

        for item_idx in last_n_items:
            candidate = self.__item_topK_similar(item_idx, topN)
            candidate_set.update(candidate)

        candidate_list = list(candidate_set)
        score_matric = np.zeros((len(last_n_items), len(candidate_list)))
        for i, item_id in enumerate(last_n_items):
            score_matric[i] = self.__item_item_arr_norm_score(item_id, candidate_list)

        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = rank_weight.dot(score_matric).tolist()
        # print(list(zip(candidate_list, final_score)))
        final_items = sorted(zip(candidate_list, final_score), key=lambda x:x[1], reverse=True)
        # print(f"[Time|Preict] {time.time()-b_time}")

        res = []
        for item, score in final_items[:topN]:
            try:
                if self.type == 'item':
                    res.append(self.item_idx_reverse[item])
                else:
                    res.append(self.item_idx_reverse[item].split(':', 1)[1])
            except:
                pass
        return res


    def __item_topK_similar(self, given_idx, topK):
        return self.t.get_nns_by_item(given_idx, topK)


    def __item_item_arr_norm_score(self, given_idx, candidate_idx_arr):
        res = [1-self.t.get_distance(given_idx, candidate_idx) for candidate_idx in candidate_idx_arr]
        print(given_idx, sorted(zip(candidate_idx_arr, res), key=lambda x:x[1], reverse=True))
        res = np.array(res)
        return  res / np.linalg.norm(res)



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10,
        'type': 'behavior',
        'item_vec_file': '../data/sample_model.vec',
        'index_file_file': '../tmp/sample'
    }
    model = W2V_ANN(config)
    model.train()
    model.test()
    model.print_metrics()