#!/usr/bin/env python

import simpy

from hdrh.histogram import HdrHistogram
import matplotlib.pyplot as plt
import numpy as np
import random


class Host(object):
    def __init__(self, env, sched, num_cores):
        self.env = env
        self.cores = [None] * num_cores
        self.sched = sched
        self.sched.set_host(self)

    def receive_request(self, request):
        # print 'Host: Received request %d at %d' % (request.idx, env.now)
        env.process(self.sched.handle_request(request))


class Scheduler(object):
    def __init__(self, env, histogram):
        self.env = env
        self.histogram = histogram
        self.queue = []

    def set_host(self, host):
        self.host = host

    def find_empty_cores(self):
        for i in range(len(self.host.cores)):
            if not self.host.cores[i]:
                return i
        return None

    def handle_request(self, request):
        self.queue.append(request)
        empty_core = self.find_empty_cores()

        while ((empty_core is not None) and len(self.queue) > 0):
            request = self.queue.pop(0)
            # print('Scheduler: Assigning request %d to core %d at %d'
                  # # % (request.idx, empty_core, env.now))
            self.host.cores[empty_core] = request
            yield env.timeout(request.exec_time)
            self.host.cores[empty_core] = None
            latency = env.now - request.start_time
            # print('Scheduler: Request %d Latency %d' % (request.idx, latency))
            self.histogram.record_value(latency)
            # print('Scheduler: Request %d finished execution at core %d at %d'
                  # % (request.idx, empty_core, env.now))
            empty_core = self.find_empty_cores()
        if (len(self.queue) > 0):
            # print('Scheduler: Can\'t schedule request %d Cores are full at %d'
                  # % (request.idx, env.now))
            yield env.timeout(0)


class Request(object):
    def __init__(self, idx, exec_time, start_time):
        self.idx = idx
        self.exec_time = exec_time
        self.start_time = start_time


class RequestGenerator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.action = env.process(self.run())

    def run(self):
        idx = 0
        while True:
            # print 'Generator: Dispatching request %d at %d' % (idx, env.now)
            self.host.receive_request(Request(idx, 7, env.now))
            yield env.timeout(1)
            idx = idx + 1

class HeavyTailRequestGenerator(RequestGenerator):

    def __init__(self, env, host, latency, tail_percent, tail_latency, num_cores):
        # Tail percent of 2 means there is 2% of requests that are of "tail latency"" value
        RequestGenerator.__init__(self, env, host)
        self.latency = latency
        self.tail_percent = tail_percent
        self.tail_latency = tail_latency
        self.scale = (tail_latency * (tail_percent / 100.0) + latency * ((100- tail_percent)/ 100.0)) / num_cores

    def run(self):
        idx = 0
        while True:
            # get how long to wait
            s = np.random.np.random.exponential(self.scale)
            yield env.timeout(s)
            # Generating request
            # Assume percentage is integer
            x = random.randint(0, 99)
            is_heavy = x < self.tail_percent
            exec_time = self.tail_latency if is_heavy else self.latency
            self.host.receive_request(Request(idx, exec_time, env.now))
            idx += 1

env = simpy.Environment()
num_cores = 100

# Track latency in the range 1us to 1sec with precision 0.01%
histogram = HdrHistogram(1, 1000 * 1000, 2)

# Initialize the different components of the system
sim_sched = Scheduler(env, histogram)
sim_host = Host(env, sim_sched, num_cores)
# sim_gen = RequestGenerator(env, sim_host)
sim_gen = HeavyTailRequestGenerator(env, sim_host, 10, 2, 80, num_cores)

# Run the simulation
env.run(until=10000)


# Ploting out values
values = []
for item in histogram.get_recorded_iterator():
        values.extend([item.value_iterated_to]* item.count_added_in_this_iter_step)

plt.hist(values)
plt.title("Latency historygram")
plt.xlabel("latency (ticks)")
plt.ylabel("Frequency")
plt.show()
