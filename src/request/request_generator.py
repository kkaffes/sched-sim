import random
import numpy as np
from request import Request


class RequestGenerator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.action = env.process(self.run())

    def run(self):
        idx = 0
        while True:
            self.host.receive_request(Request(idx, 7, self.env.now))
            yield self.env.timeout(1)
            idx += 1


class HeavyTailRequestGenerator(RequestGenerator):

    def __init__(self, env, host, exec_time, heavy_percent, heavy_exec_time,
                 num_cores, load):
        # Tail percent of 2 means that 2% of requests require "tail latency"
        # execution time, the others require "latency" execution
        # time.
        RequestGenerator.__init__(self, env, host)
        self.exec_time = exec_time
        self.heavy_exec_time = heavy_exec_time
        self.heavy_percent = heavy_percent
        inv_load = 1.0 / load
        self.scale = (inv_load * (heavy_exec_time * (heavy_percent / 100.0) +
                      exec_time * ((100 - heavy_percent) / 100.0)) / num_cores)

    def run(self):
        idx = 0
        while True:
            # Generate request
            # NOTE Percentage must be integer
            is_heavy = random.randint(0, 99) < self.heavy_percent
            exec_time = self.heavy_exec_time if is_heavy else self.exec_time
            self.host.receive_request(Request(idx, exec_time, self.env.now))

            # Generate inter-arrival time
            s = np.random.np.random.exponential(self.scale)
            yield self.env.timeout(s)
            idx += 1
