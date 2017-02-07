#!/usr/bin/env python

import simpy
import logging
import optparse
import numpy as np

import matplotlib.pyplot as plt
from hdrh.histogram import HdrHistogram

from host.host import GlobalQueueHost
# from host.host import MultiQueueHost
from request.request_generator import HeavyTailRequestGenerator


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
    sim_host = GlobalQueueHost(env, int(opts.cores), histogram)
    # sim_host = MultiQueueHost(env, int(opts.cores), histogram)
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


if __name__ == "__main__":
    main()
