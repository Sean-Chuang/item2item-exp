from abc import ABC, abstractmethod
from metrics import metrics
import numpy as np

class Model(ABC):

    def __init__(self, test_file, lastN, topN):
        self.test_file = test_file
        self.lastN = lastN
        self.topN = topN
        self.pred_next_purchase_metric = []
        self.pred_whole_day_metric = []

 
    @abstractmethod
    def train(self):
        'Build the vector for knn or others'
        return NotImplemented

    @abstractmethod
    def predict(self, last_n_events, topN):
        'Private class, support test funciton, return TopN'
        return NotImplemented


    def test(self):
        # "test_file" key is requirement
        with open(self.test_file, 'r') as in_f:
            for line in in_f:
                print(line)
                history, predict = line.strip().split('\t')
                history_events = history.split('#')
                predict_events = predict.split('#')
                self._single_user_test(history_events, predict_events)
                

    def _single_user_test(self, history_events, predict_events):
        # Get next purchase item
        purchase_items = []
        for idx, event in enumerate(predict_events):
            behavior, item = event.split(':', 1)
            if behavior == 'revenue':
                purchase_items.append((idx, item))

        # topN res
        for idx, event in enumerate(predict_events):
            print(f'---------------{idx},{event}-------------', )
            # check history_events
            while len(history_events) > self.lastN:
                history_events.pop(0)

            pred = self.predict(history_events, self.topN)
            # predict the next purchase item
            if purchase_items:
                gt = set([purchase_items[0][1]])
                metrics_map = ['HR', 'MRR', 'NDCG']
                out = metrics(set(gt), pred, metrics_map)
                print('[p] :', gt, pred, out)
                self.pred_next_purchase_metric.append(out)

            # predict the whole day items
            gt = [e.split(':', 1)[1] for e in predict_events[idx:]]
            metrics_map = ['P&R', 'MAP']
            out = metrics(set(gt), pred, metrics_map)
            print('[whole] :', gt, pred, out)
            self.pred_whole_day_metric.append(out[0] + [out[1]])

            # check purchase item
            if purchase_items and purchase_items[0][0] <= idx:
                purchase_items.pop(0)

            # prepare next history_events
            history_events.append(event)


    def print_metrics(self):
        a = np.array(self.pred_next_purchase_metric)
        b = np.array(self.pred_whole_day_metric)
        HR, MRR, NDCG = np.mean(a, axis=0).tolist()
        Precison, Recall, F1, MAP = np.mean(b, axis=0).tolist()
        print('HR\tMRR\tNDCG')
        print(f'{HR}\t{MRR}\t{NDCG}')
        print('Precison\tRecall\tF1\tMAP')
        print(f'{Precison}\t{Recall}\t{F1}\t{MAP}')