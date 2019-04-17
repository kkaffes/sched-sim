import sys

import logging
import random
from request import Request
import numpy as np


gen_dict = {
    "fixed": "FixedGenerator",
    "lognormal": "LognormalGenerator",
    "exponential": "ExponentialGenerator",
}


class FixedGenerator(object):
    def __init__(self, opts, is_network):
        if is_network:
            self.runtime = opts["network_time"]
        else:
            self.runtime = opts["app_time"]

    def mean(self):
        return self.runtime

    def sample(self):
        return self.runtime


class LognormalGenerator(object):
    def __init__(self, opts, is_network):
        if is_network:
            self.true_mean = opts["network_mean"]
            scale = float(opts["std_dev_network"] ** 2)
            self.log_mean = np.log(opts["network_mean"]**2 /
                                   np.sqrt(opts["network_mean"]**2 + scale))
            self.var = np.sqrt(np.log(scale / opts["network_mean"]**2 + 1))
        else:
            self.true_mean = opts["app_mean"]
            scale = float(opts["std_dev_app"] ** 2)
            self.log_mean = np.log(opts["app_mean"]**2 /
                                   np.sqrt(opts["app_mean"]**2 + scale))
            self.var = np.sqrt(np.log(scale / opts["app_mean"]**2 + 1))

    def mean(self):
        return self.true_mean

    def sample(self):
        return np.random.lognormal(self.log_mean, self.var)


class ExponentialGenerator(object):
    def __init__(self, mean):
        self.mean = mean

    def mean(self):
        return self.mean

    def sample(self):
        return np.random.exponential(self.mean)


class RequestGenerator(object):
    def __init__(self, env, host, num_cores, opts):
        self.env = env
        self.host = host
        self.load = opts["load"]
        self.num_cores = num_cores

        network_gen = getattr(sys.modules[__name__],
                              gen_dict[opts["network_gen"]])
        self.network_gen = network_gen(opts, True)

        app_gen = getattr(sys.modules[__name__],
                          gen_dict[opts["app_gen"]])
        self.app_gen = app_gen(opts, False)

        inter_gen = getattr(sys.modules[__name__],
                            gen_dict[opts["inter_gen"]])
        inter_mean = ((self.network_gen.mean() + self.app_gen.mean()) /
                       self.load / self.num_cores)
        self.inter_gen = inter_gen(inter_mean)

    def set_host(self, host):
        self.host = host

    def begin_generation(self):
        self.action = self.env.process(self.run())

    def set_flow_id(self, flow_id):
        self.flow_id = flow_id

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.sample()
            yield self.env.timeout(s)

            network_time = self.network_gen.sample()
            app_time = self.app_gen.sample()

            self.host.receive_request(Request(idx, app_time, network_time,
                                              self.env.now, self.flow_id))
            idx += 1


class MultipleRequestGenerator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.generators = []
        self.cur_flow_id = 0
        self.idx = 0

    def add_generator(self, gen):
        gen.set_flow_id(self.cur_flow_id)
        self.cur_flow_id += 1
        self.generators.append(gen)

    def begin_generation(self):
        for i in self.generators:
            i.set_host(self)
            i.begin_generation()

    def receive_request(self, request):
        request.idx = self.idx
        self.host.receive_request(request)
        self.idx += 1


class NetworkRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)
        self.exec_time = opts["exec_time"]
        self.network_time = opts["network_time"]

        inv_load = 1.0 / self.load
        mean = inv_load * (self.exec_time + self.network_time) / self.num_cores
        self.inter_gen = inter_gen(mean, opts)
        self.mean = self.exec_time

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            # Generate request
            # NOTE Percentage must be integer
            self.host.receive_request(Request(idx, self.exec_time,
                                              self.network_time,
                                              self.env.now, self.flow_id,
                                              self.mean))
            idx += 1


class HeavyTailRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        # Tail percent of 2 means that 2% of requests require "tail latency"
        # execution time, the others require "latency" execution
        # time.
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)
        self.exec_time = opts["exec_time"]
        self.heavy_exec_time = opts["heavy_time"]
        self.heavy_percent = opts["heavy_per"]

        self.mean = (self.heavy_exec_time * (self.heavy_percent / 100.0) +
                     self.exec_time * ((100 - self.heavy_percent) / 100.0))
        inv_load = 1.0 / self.load
        mean = inv_load * self.mean / self.num_cores
        self.inter_gen = inter_gen(mean, opts)

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            # Generate request
            # NOTE Percentage must be integer
            is_heavy = random.randint(0, 999) < self.heavy_percent * 10
            exec_time = self.heavy_exec_time if is_heavy else self.exec_time
            self.host.receive_request(Request(idx, exec_time, 0, self.env.now,
                                              self.flow_id, self.mean))

            idx += 1


class ExponentialRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)
        self.mean = float(opts["mean"])
        arrival_mean = self.mean / self.load / self.num_cores
        self.inter_gen = inter_gen(arrival_mean, opts)

    def run(self):
        idx = 0
        while True:

            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = np.random.exponential(self.mean)

            self.host.receive_request(Request(idx, exec_time, 0, self.env.now,
                                              self.flow_id, self.mean))

            idx += 1


class LogNormalRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)

        self.scale = float(opts["std_dev_request"] ** 2)
        self.mean = np.log(opts["mean"]**2 / np.sqrt(opts["mean"]**2 +
                                                     self.scale))
        self.var = np.sqrt(np.log(self.scale / opts["mean"]**2 + 1))

        arrival_mean = opts["mean"] / self.load / self.num_cores

        self.inter_gen = inter_gen(arrival_mean, opts)
        self.log_mean = opts["mean"]

    def run(self):
        idx = 0
        while True:

            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = np.random.lognormal(self.mean, self.var)

            self.host.receive_request(Request(idx, exec_time, self.env.now,
                                              self.flow_id, self.log_mean))

            idx += 1


class LogNormalNetworkLogNormalRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)

        self.scale = float(opts["std_dev_request"] ** 2)
        self.mean = np.log(opts["mean"]**2 / np.sqrt(opts["mean"]**2 +
                                                     self.scale))
        self.var = np.sqrt(np.log(self.scale / opts["mean"]**2 + 1))

        self.network_scale = float(opts["std_dev_network"] ** 2)
        self.network_mean = np.log(opts["network_mean"]**2 /
                                   np.sqrt(opts["network_mean"]**2 +
                                           self.network_scale))
        self.network_var = np.sqrt(np.log(self.network_scale /
                                          opts["network_mean"]**2 + 1))

        arrival_mean = (opts["mean"] + opts["network_mean"])\
                       / self.load / self.num_cores

        self.inter_gen = inter_gen(arrival_mean, opts)
        self.log_mean = opts["mean"]

    def run(self):
        idx = 0
        while True:

            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = np.random.lognormal(self.mean, self.var)
            network_time = np.random.lognormal(self.network_mean,
                                               self.network_var)

            self.host.receive_request(Request(idx, exec_time, network_time,
                                              self.env.now, self.flow_id,
                                              self.log_mean))

            idx += 1


class NetworkLogNormalRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)

        self.scale = float(opts["std_dev_request"] ** 2)
        self.mean = np.log(opts["mean"]**2 / np.sqrt(opts["mean"]**2 +
                                                     self.scale))
        self.var = np.sqrt(np.log(self.scale / opts["mean"]**2 + 1))

        self.network_time = opts["network_time"]

        arrival_mean = (opts["mean"] + opts["network_time"])\
                       / self.load / self.num_cores

        self.inter_gen = inter_gen(arrival_mean, opts)
        self.log_mean = opts["mean"]

    def run(self):
        idx = 0
        while True:

            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = np.random.lognormal(self.mean, self.var)

            self.host.receive_request(Request(idx, exec_time,
                                              self.network_time,
                                              self.env.now, self.flow_id,
                                              self.log_mean))

            idx += 1


class LogNormalNetworkRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)
        self.exec_time = opts["exec_time"]

        self.network_scale = float(opts["std_dev_network"] ** 2)
        self.network_mean = np.log(opts["network_mean"]**2 /
                                   np.sqrt(opts["network_mean"]**2 +
                                           self.network_scale))
        self.network_var = np.sqrt(np.log(self.network_scale /
                                          opts["network_mean"]**2 + 1))

        inv_load = 1.0 / self.load
        mean = inv_load * (self.exec_time + opts["network_mean"])\
                / self.num_cores
        self.inter_gen = inter_gen(mean, opts)
        self.mean = self.exec_time


    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            # Generate request
            # NOTE Percentage must be integer
            network_time = np.random.lognormal(self.network_mean,
                                               self.network_var)
            self.host.receive_request(Request(idx, self.exec_time,
                                              network_time, self.env.now,
                                              self.flow_id, self.mean))
            idx += 1


class ParetoRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)

        self.scale = 1 + np.sqrt(1.0 + opts["mean"]**2 /
                                 opts["std_dev_request"]**2)
        self.mu = (self.scale - 1) * opts["mean"] / self.scale

        arrival_mean = opts["mean"] / self.load / self.num_cores

        self.inter_gen = inter_gen(arrival_mean, opts)
        self.mean = opts["mean"]

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = (np.random.pareto(self.scale) + 1) * self.mu
            self.host.receive_request(Request(idx, exec_time, self.env.now,
                                              self.flow_id, self.mean))
            idx += 1


class NormalRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)

        self.mu = opts["mean"]
        self.std = opts["std_dev_request"]
        self.inter_gen = inter_gen(opts["mean"] / self.load / self.num_cores,
                                   opts)
        self.mean = opts["mean"]

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            exec_time = -1
            while (exec_time < 0 or exec_time > 2 * self.mu):
                exec_time = np.random.normal(self.mu, self.std)

            self.host.receive_request(Request(idx, exec_time, self.env.now,
                                              self.flow_id, self.mean))
            idx += 1
