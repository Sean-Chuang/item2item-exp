from abc import ABC, abstractmethod

class Model(abc.ABC):
    @abstractmethod
    def config(self, config):
        'Model corresponding setting'
        return NotImplemented
 
    @abstractmethod
    def train(self):
        'Build the vector for knn or others'
        return NotImplemented

    @abstractmethod
    def __predict(self, last_n_events, topN):
        'Private class, support test funciton, return TopN'
        return NotImplemented


    def test(self):
        # "test_file" key is requirement
        with open(self.config['test_file'], 'r') as in_f:
            for line in in_f:
                history, predict = line.strip().split('\t')
                history_events = history.split('#')
                predict_events = predict.split('#')
                # Get next purchase item
                purchase_items = []
                for idx, event in enumerate(predict_events):
                    behavior, item = event.split(':', 1)
                    if behavior == 'revenue':
                        purchase_items.append((idx, item))

                # topN res
                hit_score, mrr_score = 0, 0
                for idx, event in enumerate(predict_events):
                    topN = self.__predict(history_events)
                    # predict the next purchase item
                    if purchase_items:
                        if purchase_items[0][0] <= idx:
                            purchase_items.pop(0)

                        if purchase_items and purchase_items[0][1] in topN:
                            hit_score.append(1)
                            rank = topN.index(purchase_items[0][1])
                            mrr_score.append(1/(1+rank))

                    # predict the whole day items
                    gt = [event.split(':', 1)[1] for e in predict_events[idx:]]



    def print_metrics(self):
        pass



# class S-POP(Model):
#     def train(self):
#         'Return when animal screaming the sound hear likes'
#         return NotImplemented
 
#     @abstractmethod
#     def test(self):
#         'Make animal walk to position (x, y).'
#         return NotImplemented

#     @abstractmethod
#     def print_metrics(self):
#         'Make animal walk to position (x, y).'
#         return NotImplemented