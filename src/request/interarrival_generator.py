import numpy as np


class InterArrivalGenerator(object):
    def __init__(self, mean, scale=None):
        self.mean = mean
        self.scale = scale

    def next(self):
        return 1


class PoissonGenerator(InterArrivalGenerator):
    def next(self):
        return np.random.exponential(self.mean)


class LogNormalGenerator(InterArrivalGenerator):
    def __init__(self, mean, scale=None):
        InterArrivalGenerator.__init__(self, mean, scale)
        self.scale = float(scale)
        # Calculate the mean of the underlying normal distribution
        self.mean = np.log(mean**2 / np.sqrt(mean**2 + self.scale))
        self.scale = np.sqrt(np.log(self.scale / mean**2 + 1))

    def next(self):
        return np.random.lognormal(self.mean, self.scale)
