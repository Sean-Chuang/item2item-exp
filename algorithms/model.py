from abc import ABC, abstractmethod

class Model(abc.ABC):

    def config(self, config):
        self.config = config
 
    @abstractmethod
    def train(self):
        'Return when animal screaming the sound hear likes'
        return NotImplemented
 
    @abstractmethod
    def test(self):
        'Make animal walk to position (x, y).'
        return NotImplemented

    @abstractmethod
    def print_metrics(self):
        'Make animal walk to position (x, y).'
        return NotImplemented


class POP(Model):

    def train(self):
        'Return when animal screaming the sound hear likes'
        return NotImplemented
 
    @abstractmethod
    def test(self):
        'Make animal walk to position (x, y).'
        return NotImplemented

    @abstractmethod
    def print_metrics(self):
        'Make animal walk to position (x, y).'
        return NotImplemented