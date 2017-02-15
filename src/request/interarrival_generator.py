import numpy as np


class InterArrivalGenerator(object):
    def __init__(self, mean, opts=None):
        self.mean = mean

    def next(self):
        return 1


class PoissonArrivalGenerator(InterArrivalGenerator):
    def next(self):
        return np.random.exponential(self.mean)


class LogNormalArrivalGenerator(InterArrivalGenerator):
    def __init__(self, mean, opts=None):
        InterArrivalGenerator.__init__(self, mean, opts)
        self.scale = float(opts["std_dev_arrival"]**2)
        # Calculate the mean of the underlying normal distribution
        self.mean = np.log(mean**2 / np.sqrt(mean**2 + self.scale))
        self.scale = np.sqrt(np.log(self.scale / mean**2 + 1))

    def next(self):
        return np.random.lognormal(self.mean, self.scale)
