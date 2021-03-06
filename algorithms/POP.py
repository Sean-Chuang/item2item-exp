from model import Model
import numpy as np

class POP(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN', 'item_pop_file']
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
        self.pop_items = []
        with open(self.config['item_pop_file'], 'r') as in_f:
            for line in in_f:
                item, pop = line.strip().split('\t')
                self.pop_items.append(item)
                if len(self.pop_items) > self.config['topN']:
                    break
        print("Train finished ...")
 

    def predict(self, last_n_events, topN):
        return self.pop_items[:topN]



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10, 
        'item_pop_file':'../data/items_purchase_freq.csv'
    }
    model = POP(config)
    model.train()
    model.test()
    model.print_metrics()