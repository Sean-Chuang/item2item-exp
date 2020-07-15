from model import Model
import numpy as np
from tqdm import tqdm
from pyhive import presto
import pandas as pd

class POP(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 'dt']
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

    def train(self):
        b_time = time.time()
        cursor = presto.connect('presto.smartnews.internal',8081).cursor()
        query = f"""
            select 
                content_id,
                count(*) as count
            from z_seanchuang.i2i_offline_train_raw 
            where dt = '{config['dt']}'
              and content_id != ''
              and behavior_types = '{config['behavior']}'
            group by 1
            order by 2 desc
            limit {config['topN']}
        """
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        self.pop_items = df['content_id'].tolist()
        print("Train finished ... : ", time.time() - b_time)
 

    def predict(self, last_n_events, topN):
        return self.pop_items[:topN]



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10, 
        'dt': '2020-06-30',
        'behavior': 'ViewContent'
    }
    model = POP(config)
    model.train()
    model.test()
    model.print_metrics()