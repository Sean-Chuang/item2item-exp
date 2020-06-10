from model import Model

class POP(Model):
    requirement = ['item_pop_file', 'test_file', 'topN']
    def config(self, config):
        self.config = config
        miss = set()
        for item in requirement:
            if item not in self.config:
                miss.add(item)
        if len(miss) > 0:
            raise Exception(f"Miss the key : {miss}")

    def train(self):
        self.pop_items = []
        with open(self.config['item_pop_file'], 'r') as in_f:
            for line in in_f:
                item, pop = line.strip().split('\t')
                self.pop_items.append(item)
                if len(self.pop_items) > self.config['topN']:
                    break
        print("Train finished ...")
 
    def __predict(self, last_n_events):
        return self.pop_items[:self.config['topN']]

    def test(self):
        with open(self.config['test_file'], 'r') as in_f:
            for line in in_f:
                history, predict = line.strip().split('\t')
                self.history_items.append(history.split('#'))
                self.predict_items.append(predict.split('#'))

    @abstractmethod
    def print_metrics(self):
        'Make animal walk to position (x, y).'
        return NotImplemented