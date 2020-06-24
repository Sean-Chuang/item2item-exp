from model import Model
import numpy as np
from annoy import AnnoyIndex
import time
from tqdm import tqdm

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
        self.action_w = {
            'ViewContent' : 1.0,
            'AddToCart' : 3.0,
            'revenue' : 6.0
        }


    def train(self):
        b_time = time.time()
        self.item_idx = {}
        self.item_idx_reverse = {}
        tmp_vector = {}
        with open(self.config['item_vec_file'], 'r') as in_f:
            num_items, dim = in_f.readline().strip().split()
            print(f'Num of items : {num_items}, dim : {dim}')
            self.t = AnnoyIndex(int(dim), 'angular')
            
            for idx, line in tqdm(enumerate(in_f)):
                tmp = line.split()
                if self.type == 'item':
                    try:
                        action, item_org = tmp[0].split(':', 1)
                    except:
                        continue
                else:
                    action = None
                    item_org = tmp[0]

                if item_org not in self.item_idx:
                    self.item_idx[item_org] = idx
                    self.item_idx_reverse[idx] = item_org
                    tmp_vector[idx] = np.zeros(int(dim))
                else:
                    idx = self.item_idx[item_org]

                if self.type == 'item':
                    tmp_vector[idx] += np.array(tmp[1:], dtype=float) * self.action_w[action]
                else:
                    tmp_vector[idx] += np.array(tmp[1:], dtype=float)

        for idx in tmp_vector:
            self.t.add_item(idx, tmp_vector[idx].tolist())
        print("Read file finished ...")
        file_name = self.config['index_file_file'] + '.' + self.type 

        self.t.build(30) # 10 trees
        self.t.save(f'{file_name}.ann')

        # self.t.load(f'{file_name}.ann')

        print(f"Train finished ...{time.time() - b_time}")
 

    def predict(self, last_n_events, topN):
        b_time = time.time()
        item_candidate = list()
        if self.type == 'item':
            last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1] if e.split(':', 1)[1] in self.item_idx]
        else:
            last_n_items = [self.item_idx[e] for e in last_n_events[::-1] if e in self.item_idx]
        
        if len(last_n_items) == 0:
            return []

        for item_idx in last_n_items:
            item_candidate.append(self.__item_topK_similar(item_idx, topN))


        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        final_score = []
        for i, _res in enumerate(item_candidate):
            for item in _res:
                final_score.append((item, _res[item] * rank_weight[i]))

        # print(last_n_items, list(zip(candidate_list, final_score)))
        final_items = sorted(final_score, key=lambda x:x[1], reverse=True)
        # print(f"[Time|Preict] {time.time()-b_time}")

        res = []
        for item, score in final_items:
            try:
                if self.type == 'item':
                    item_raw = self.item_idx_reverse[item]
                else:
                    item_raw = self.item_idx_reverse[item].split(':', 1)[1]

                if item_raw in res:
                    continue
                res.append(item_raw)
            except:
                pass
            if len(res) == topN:
                break
        return res


    def __item_topK_similar(self, given_idx, topK):
        item_idx_arr, score_arr = self.t.get_nns_by_item(given_idx, topK, include_distances=True)
        res = {}
        for idx, score in zip(item_idx_arr, score_arr):
            if self.type == 'item':
                item_raw = self.item_idx_reverse[idx]
            else:
                item_raw = self.item_idx_reverse[idx].split(':', 1)[1]
            res[item_raw] = score
        return res




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