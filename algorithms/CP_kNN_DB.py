# Condition Probability Knn
from model import Model
import numpy as np
from collections import defaultdict
from tqdm import tqdm
from pyhive import presto
import pandas as pd
import time, sys

class CP_kNN(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 'type', 'dt']
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
        self.dt = config['dt']


    def train(self):
        b_time = time.time()
        cursor = presto.connect('presto.smartnews.internal',8081).cursor()
        #query = f"select * from hive_ad.z_seanchuang.i2i_offline_item_topk_items where dt='{self.dt}'"
        query = f"""
            with test_item as (
                select 
                    distinct content_id as item
                from hive_ad.z_seanchuang.i2i_offline_test_raw
                where dt = '{self.dt}'
            )
            select 
                all.* 
            from hive_ad.z_seanchuang.i2i_offline_item_topk_items all
            inner join test_item test
                on all.item = test.item
                  and all.dt='{self.dt}'
        """
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        self.items_similar = df.set_index('item')['topk_json'].to_dict(defaultdict(list))
        print("Train finished ... : ", time.time() - b_time)


    def predict(self, last_n_events, topN):
        candidate_set = set()

        if self.type == 'item':
            last_n_items = [e.split(':', 1)[1] for e in last_n_events[::-1]]
        else:
            last_n_items = [e for e in last_n_events[::-1]]
        
        for item_idx in last_n_items:
            _similar_res = self.__item_similarity(item_idx, topN+5)
            candidate_set.update([_item for _item, score in _similar_res])

        candidate_set -= set(last_n_items)
        # print('candidate_set', candidate_set)
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
        return [item for item, score in final_items[:topN]]


    def __item_similarity(self, item, topK):
        res = [x.split('=') for x in self.items_similar[item]]
        return res


    def __item_item_arr_norm_score(self, item, candidate_item_arr):
        res = np.zeros(len(candidate_item_arr))
        for x in self.items_similar[item]:
            _item, _score = x.split('=')
            if _item in candidate_item_arr:
                res[candidate_item_arr.index(_item)] = float(_score)
        return res / np.linalg.norm(res)



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10,
        'type': 'item',
        'dt' : '2020-06-30',
    }
    model = CP_kNN(config)
    model.train()
    model.test()
    model.print_metrics()