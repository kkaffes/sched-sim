#!/usr/bin/env python

import simpy

from hdrh.histogram import HdrHistogram


class Host(object):
    def __init__(self, env, sched, num_cores):
        self.env = env
        self.cores = [None] * num_cores
        self.sched = sched
        self.sched.set_host(self)

    def receive_request(self, request):
        print 'Host: Received request %d at %d' % (request.idx, env.now)
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
            print('Scheduler: Assigning request %d to core %d at %d'
                  % (request.idx, empty_core, env.now))
            self.host.cores[empty_core] = request
            yield env.timeout(request.exec_time)
            self.host.cores[empty_core] = None
            latency = env.now - request.start_time
            print('Scheduler: Request %d Latency %d' % (request.idx, latency))
            self.histogram.record_value(latency)
            print('Scheduler: Request %d finished execution at core %d at %d'
                  % (request.idx, empty_core, env.now))
            empty_core = self.find_empty_cores()
        if (len(self.queue) > 0):
            print('Scheduler: Can\'t schedule request %d Cores are full at %d'
                  % (request.idx, env.now))
            yield env.timeout(0)


class Request(object):
    def __init__(self, idx, exec_time, start_time):
        self.idx = idx
        self.exec_time = exec_time
        self.start_time = start_time


class Request_Generator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.action = env.process(self.run())

    def run(self):
        idx = 0
        while True:
            print 'Generator: Dispatching request %d at %d' % (idx, env.now)
            self.host.receive_request(Request(idx, 4, env.now))
            yield env.timeout(5)
            idx = idx + 1


env = simpy.Environment()
num_cores = 2

# Track latency in the range 1us to 1sec with precision 0.01%
histogram = HdrHistogram(1, 1000 * 1000, 2)

# Initialize the different components of the system
sim_sched = Scheduler(env, histogram)
sim_host = Host(env, sim_sched, num_cores)
sim_gen = Request_Generator(env, sim_host)

# Run the simulation
env.run(until=100)
