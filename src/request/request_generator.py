import random
import numpy as np
from request import Request


class RequestGenerator(object):
    def __init__(self, env, host, load, num_cores):
        self.env = env
        self.host = host
        self.load = load
        self.num_cores = num_cores
        self.action = env.process(self.run())

    def run(self):
        idx = 0
        while True:
            self.host.receive_request(Request(idx, 7, self.env.now))
            yield self.env.timeout(1)
            idx += 1


class HeavyTailRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, load, num_cores, opts):
        # Tail percent of 2 means that 2% of requests require "tail latency"
        # execution time, the others require "latency" execution
        # time.
        RequestGenerator.__init__(self, env, host, load, num_cores)
        self.exec_time = int(opts.exec_time)
        self.heavy_exec_time = int(opts.heavy_time)
        self.heavy_percent = int(opts.heavy_per)

        inv_load = 1.0 / self.load
        mean = (inv_load * (self.heavy_exec_time * (self.heavy_percent /
                100.0) + self.exec_time * ((100 - self.heavy_percent) /
                100.0)) / self.num_cores)
        self.inter_gen = inter_gen(mean, opts.std_dev)

    def run(self):
        idx = 0
        while True:
            # Generate request
            # NOTE Percentage must be integer
            is_heavy = random.randint(0, 99) < self.heavy_percent
            exec_time = self.heavy_exec_time if is_heavy else self.exec_time
            self.host.receive_request(Request(idx, exec_time, self.env.now))

            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)
            idx += 1


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
        self.scale = int(scale)
        # Calculate the mean of the underlying normal distribution
        self.mean = np.log(mean**2 / np.sqrt(mean**2 + self.scale))
        self.scale = np.sqrt(np.log(self.scale / mean**2 + 1))

    def next(self):
        return np.random.lognormal(self.mean, self.scale)
