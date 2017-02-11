#!/usr/bin/env python

import sys
import json
import simpy
import logging
import optparse

import matplotlib.pyplot as plt
from util.histogram import Histogram

from host.host import *
from request.request_generator import *
from request.interarrival_generator import *


gen_dict = {
    'heavy_tail': 'HeavyTailRequestGenerator',
    'poisson': 'PoissonGenerator',
    'lognormal': 'LogNormalGenerator',
    'global': 'GlobalQueueHost',
    'local': 'MultiQueueHost',
}


def main():
    parser = optparse.OptionParser()

    parser.add_option('-v', '--verbose', dest='verbose',
                      action='count', help='Increase verbosity (specify'
                      ' multiple times for more)')
    parser.add_option('-g', '--print-hist', action='store_true', dest='hist',
                      help='Print request latency histogram', default=False)
    parser.add_option('-c', '--cores', dest='cores', action='store',
                      help='Set the number of cores of the system', default=8)
    parser.add_option('--workload-conf', dest='work_conf', action='store',
                      help='Configuration file for the load generation'
                      ' functions', default="../config/work.json")

    group = optparse.OptionGroup(parser, 'Scheduler Options')
    group.add_option('--time-slice', dest='time_slice', action='store',
                     help='Set the maximum number of ticks a request is'
                     ' allowed to run in a processor without being preempted'
                     ' (set to 0 for no-preemption)', default=0.0)
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

    # Initialize the different components of the system
    env = simpy.Environment()

    # Parse the configuration file
    flow_config = json.loads(open(opts.work_conf).read())

    # Create a histogram per flow and a global histogram
    histograms = Histogram(len(flow_config))

    # Get the queue configuration
    queue_conf = getattr(sys.modules[__name__], gen_dict[opts.queue])
    sim_host = queue_conf(env, int(opts.cores), float(opts.deq_cost),
                          float(opts.time_slice), histograms)

    # sim_host = ShinjukuHost(env, int(opts.cores),
                            # float(opts.time_slice), histograms)

    multigenerator = MultipleRequestGenerator(env, sim_host)

    # Create one object per flow
    for flow in flow_config:
        params = flow_config[flow]
        inter_gen = getattr(sys.modules[__name__],
                            gen_dict[params["inter_gen"]])
        work_gen = getattr(sys.modules[__name__],
                           gen_dict[params["work_gen"]])
        multigenerator.add_generator(work_gen(env, sim_host, inter_gen,
                                              int(opts.cores), params))

    multigenerator.begin_generation()

    # Run the simulation
    env.run(until=50000)

    if opts.hist:
        # Ploting out values
        # TODO. update if wanna see histogram
        values = []
        for item in histogram[0].get_recorded_iterator():
            values.extend([item.value_iterated_to] *
                          item.count_added_in_this_iter_step)
        plt.hist(values)
        plt.title('Latency histogram')
        plt.xlabel('Latency (ticks)')
        plt.ylabel('Frequency')
        plt.show()

    # Print 99% latency
    histograms.print_percentile(99)


if __name__ == "__main__":
    main()
