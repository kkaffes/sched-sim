#!/usr/bin/env python

import simpy
import random
import logging
import optparse

import numpy as np
import matplotlib.pyplot as plt

from hdrh.histogram import HdrHistogram


class Host(object):
    def __init__(self, env, sched, num_cores):
        self.env = env
        self.cores = [None] * num_cores
        self.sched = sched
        self.sched.set_host(self)

    def receive_request(self, request):
        logging.debug('Host: Received request %d at %d' % (request.idx,
                      self.env.now))
        self.env.process(self.sched.handle_request(request))


class Scheduler(object):
    def __init__(self, env, histogram):
        self.env = env
        self.histogram = histogram
        self.queue = []
        self.request_number = 0

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
            logging.debug('Scheduler: Assigning request %d to core %d at %d'
                          % (request.idx, empty_core, self.env.now))
            self.host.cores[empty_core] = request
            yield self.env.timeout(request.exec_time)
            self.host.cores[empty_core] = None
            latency = self.env.now - request.start_time
            logging.debug('Scheduler: Request %d Latency %d' %
                          (request.idx, latency))
            self.histogram.record_value(latency)
            logging.debug('Scheduler: Request %d finished execution at core %d'
                          ' at %d' % (request.idx, empty_core, self.env.now))
            self.request_number = self.request_number + 1
            empty_core = self.find_empty_cores()
        if (len(self.queue) > 0):
            logging.debug('Scheduler: Can\'t schedule request %d Cores are'
                          ' full at %d' % (request.idx, self.env.now))
            yield self.env.timeout(0)


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


def main():
    parser = optparse.OptionParser()

    parser.add_option('-v', '--verbose', dest='verbose',
                      action='count', help='Increase verbosity (specify'
                      ' multiple times for more)')
    parser.add_option('-g', '--print_hist', action='store_true', dest='hist',
                      help='Print request latency histogram', default=False)
    parser.add_option('-x', '--exec_time', dest='exec_time',
                      action='store', help='Set the base request execution'
                      ' time', default=10)
    parser.add_option('-p', '--heavy_per', dest='heavy_per', action='store',
                      help='Set the percentage of heavy requests', default=2)
    parser.add_option('--heavy_time', dest='heavy_time', action='store',
                      help='Set the execution time of heavy requests',
                      default=80)
    parser.add_option('-c', '--cores', dest='cores', action='store',
                      help='Set the number of cores of the system', default=8)
    parser.add_option('-l', '--load', dest='load', action='store',
                      help='Set the load of the system', default=1)
    opts, args = parser.parse_args()

    # Setup logging
    log_level = logging.WARNING
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level)

    # Track latency in the range 1us to 1sec with precision 0.01%
    histogram = HdrHistogram(1, 1000 * 1000, 2)

    # Initialize the different components of the system
    env = simpy.Environment()
    sim_sched = Scheduler(env, histogram)
    sim_host = Host(env, sim_sched, int(opts.cores))
    sim_gen = HeavyTailRequestGenerator(env, sim_host, int(opts.exec_time),
                                        int(opts.heavy_per),
                                        int(opts.heavy_time), int(opts.cores),
                                        int(opts.load))

    # Run the simulation
    env.run(until=50000)

    if opts.hist:
        # Ploting out values
        values = []
        for item in histogram.get_recorded_iterator():
            values.extend([item.value_iterated_to] *
                          item.count_added_in_this_iter_step)
        plt.hist(values)
        plt.title('Latency histogram')
        plt.xlabel('Latency (ticks)')
        plt.ylabel('Frequency')
        plt.show()

    # Print 99% latency and throughput
    print histogram.get_value_at_percentile(99)
    print sim_sched.request_number


if __name__ == "__main__":
    main()
