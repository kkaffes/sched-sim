#!/usr/bin/env python

import sys
import simpy
import logging
import optparse
import numpy as np

import matplotlib.pyplot as plt
from hdrh.histogram import HdrHistogram

from host.host import *
from request.request_generator import *


gen_dict = {
    'heavy_tail': 'HeavyTailRequestGenerator',
    'poisson': 'PoissonGenerator',
    'lognormal': 'LogNormalGenerator',
    'global': 'GlobalQueueHost',
    'local': 'MultiQueueHost'}


def main():
    parser = optparse.OptionParser()

    parser.add_option('-v', '--verbose', dest='verbose',
                      action='count', help='Increase verbosity (specify'
                      ' multiple times for more)')
    parser.add_option('-g', '--print-hist', action='store_true', dest='hist',
                      help='Print request latency histogram', default=False)
    parser.add_option('-c', '--cores', dest='cores', action='store',
                      help='Set the number of cores of the system', default=8)
    parser.add_option('-l', '--load', dest='load', action='store',
                      help='Set the load of the system', default=1)
    parser.add_option('--work-gen', dest='work_gen', help='Set the request'
                      ' execution time generation function (heavy_tail)',
                      action='store', default="heavy_tail")
    parser.add_option('--inter-gen', dest='inter_gen', help='Set the'
                      ' request inter-arrival time generation function'
                      ' (poisson, lognormal)', action='store',
                      default='poisson')

    group = optparse.OptionGroup(parser, 'Heavy Tail Distribution Options')
    group.add_option('-x', '--exec_time', dest='exec_time',
                     action='store', help='Set the base request execution'
                     ' time', default=10)
    group.add_option('-p', '--heavy-per', dest='heavy_per', action='store',
                     help='Set the percentage of heavy requests', default=2)
    group.add_option('--heavy-time', dest='heavy_time', action='store',
                     help='Set the execution time of heavy requests',
                     default=80)
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Interarrival Distribution Options')
    group.add_option('--std-dev', dest='std_dev', action='store', help='Set'
                     ' the standard deviation of the interarrival time',
                     default=1)
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Queue Options')
    group.add_option('-q', '--queue', dest='queue', action='store',
                     help='Set the queue configuration (global, local)',
                     default='global')
    group.add_option('--deq-cost', dest='deq_cost', action='store',
                     help='Set the dequeuing cost', default=0.0)
    parser.add_option_group(group)

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

    # Get the queue configuration
    queue_conf = getattr(sys.modules[__name__], gen_dict[opts.queue])
    sim_host = queue_conf(env, int(opts.cores), float(opts.deq_cost),
                          histogram)

    # Get the workload generation classes
    inter_gen = getattr(sys.modules[__name__], gen_dict[opts.inter_gen])
    work_gen = getattr(sys.modules[__name__], gen_dict[opts.work_gen])

    # Create the workload generator
    sim_gen = work_gen(env, sim_host, inter_gen, float(opts.load),
                       int(opts.cores), opts)

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
