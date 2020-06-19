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


    def train(self):
        b_time = time.time()
        self.item_idx = {}
        self.item_idx_reverse = {}

        with open(self.config['item_vec_file'], 'r') as in_f:
            num_items, dim = in_f.readline().strip().split()
            print(f'Num of items : {num_items}, dim : {dim}')
            self.t = AnnoyIndex(int(dim), 'angular')
            
            for idx, line in tqdm(enumerate(in_f)):
                tmp = line.split()
                self.item_idx[tmp[0]] = idx
                self.item_idx_reverse[idx] = tmp[0]
                self.t.add_item(idx, list(map(float, tmp[1:])))
        print("Read file finished ...")
        file_name = self.config['index_file_file'] + '.' + self.type 

        self.t.build(30) # 10 trees
        self.t.save(f'{file_name}.ann')

        # self.t.load(f'{file_name}.ann')

        print(f"Train finished ...{time.time() - b_time}")
 

    def predict(self, last_n_events, topN):
        b_time = time.time()
        candidate_set = set()
        if self.type == 'item':
            last_n_items = [self.item_idx[e.split(':', 1)[1]] for e in last_n_events[::-1] if e in self.item_idx]
        else:
            last_n_items = [self.item_idx[e] for e in last_n_events[::-1] if e in self.item_idx]
        
        if len(last_n_items) == 0:
            return []

        rank_weight = np.array([1 / np.log2(rank + 2) for rank in range(len(last_n_items))])
        # Calculate session vector
        session_vec = np.mean([np.array(self.t.get_item_vector(e))*rank_weight[idx] for idx, e in enumerate(last_n_items)], axis=0)
        r_items, r_scores = self.t.get_nns_by_vector(session_vec, topN*2, include_distances=True)

        res = []
        for item in r_items:
            if item in last_n_items:
                continue

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