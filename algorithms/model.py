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
    def __predict(self, last_n_events):
        'Private class, support test funciton, return TopN'
        return NotImplemented

    @abstractmethod
    def test(self):
        'Evaluation the test file.'
        return NotImplemented

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