from model import Model
import numpy as np

class S_POP(Model):
    
    def __init__(self, config):
        self.requirement = ['test_file', 'lastN', 'topN']
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
        print("Train finished ...")
 

    def predict(self, last_n_events, topN):
        return [e.split(':', 1)[1] for e in last_n_events[-topN:]]



if __name__ == '__main__':
    config = {
        'test_file': '../te_data.csv', 
        'lastN': 10, 
        'topN': 10, 
    }
    model = S_POP(config)
    model.train()
    model.test()
    model.print_metrics()