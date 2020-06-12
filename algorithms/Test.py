from model import Model
import numpy as np
from random import randint as r

class Test(Model):
    requirement = ['test_file', 'lastN', 'topN']

    def __init__(self, config):
        self.config = config
        miss = set()
        for item in Test.requirement:
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
        # return self.pop_items[:topN]
        print(f'last_n_events: {last_n_events}')
        array = [ str(r(1,11)) for i in range(topN)]
        return array


    def test_single_user(self, histroy_events, predict_events):
        self._single_user_test(histroy_events, predict_events)


if __name__ == '__main__':
    config = {'test_file': 'test', 'lastN': 5, 'topN': 5}
    model = Test(config)
    histroy_events = ['v:1', 'v:2', 'revenue:3', 'v:4', 'v:5']
    predict_events = ['v:6', 'v:7', 'v:8', 'revenue:9', 'v:10', 'revenue:11']
    model.predict(None, 10)
    model.test_single_user(histroy_events, predict_events)
    model.print_metrics()
