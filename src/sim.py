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
    'poisson_arrival': 'PoissonArrivalGenerator',
    'lognormal_arrival': 'LogNormalArrivalGenerator',
    'exponential_request': 'ExponentialRequestGenerator',
    'lognormal_request': 'LogNormalRequestGenerator',
    'normal_request': 'NormalRequestGenerator',
    'pareto_request': 'ParetoRequestGenerator',
    'global': 'GlobalQueueHost',
    'local': 'MultiQueueHost',
    'shinjuku':  'ShinjukuHost',
    'perflow': 'PerFlowQueueHost',
    'staticcore': 'StaticCoreAllocationHost'
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

    group = optparse.OptionGroup(parser, 'Host Options')
    group.add_option('--host-type', dest='host_type', action='store',
                     help=('Set the host configuration (global queue,'
                           ' local queue, shinjuku, per flow queues,'
                           ' static core allocation)'), default='global')
    group.add_option('--deq-cost', dest='deq_cost', action='store',
                     help='Set the dequeuing cost', default=0.0)
    group.add_option('--queue-policy', dest='queue_policy', action='store',
                     help=('Set the queue policy to be followed by the per'
                           ' flow queue, ignored in any other queue'
                           ' configuration'), default='FlowQueues')
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
    histograms = Histogram(len(flow_config), float(opts.cores), flow_config)

    # Get the queue configuration
    host_conf = getattr(sys.modules[__name__], gen_dict[opts.host_type])
    sim_host = host_conf(env, int(opts.cores), histograms,
                         float(opts.deq_cost), flow_config, opts)

    # TODO:Update so that it's parametrizable
    # print "Warning: Need to update sim.py for parameterization and Testing"
    # First list is time slice, second list is load
    # sim_host = StaticCoreAllocationHost(env, int(opts.cores),
    #                                     float(opts.deq_cost), [0.0, 0.0],
    #                                     histograms, len(flow_config),
    #                                     [0.4, 0.4])

    multigenerator = MultipleRequestGenerator(env, sim_host)

    # Create one object per flow
    for flow in flow_config:
        params = flow
        inter_gen = getattr(sys.modules[__name__],
                            gen_dict[params["inter_gen"]])
        work_gen = getattr(sys.modules[__name__],
                           gen_dict[params["work_gen"]])

        # Need to generate less load when we have shinjuku because one
        # of the cores is just the dispatcher
        if (opts.host_type == "shinjuku"):
            opts.cores = int(opts.cores) - 1

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

    # Print results in json format
    histograms.print_info()

if __name__ == "__main__":
    main()
