#!/usr/bin/env python

import json
from hdrh.histogram import HdrHistogram


class Histogram(object):

    def __init__(self, num_histograms, cores, flow_config, opts):
        self.histograms = [HdrHistogram(1, 1000 * 1000, 2)
                           for i in range(num_histograms)]
        self.global_histogram = HdrHistogram(1, 1000 * 1000, 2)
        self.cores = cores
	self.flow_config = flow_config
	self.violations = [0 for i in range(len(flow_config))]
	self.dropped = [0 for i in range(len(flow_config))]
        self.print_values = opts.print_values
        if self.print_values:
            self.print_files = [open(opts.output_file + '_flow' + str(flow),
                                     'w+') for flow in range(len(flow_config))]

    def record_value(self, flow, value):
        self.global_histogram.record_value(value)
        self.histograms[flow].record_value(value)
	if self.flow_config[flow].get('slo'):
	    if value > self.flow_config[flow].get('slo'):
	        self.violations[flow] += 1
        self.print_files[flow].write(str(value) + '\n')

    def print_info(self):
        info = []
        for i in range(len(self.histograms)):
            # Add the dropped requests as max time
            max_value = self.histograms[i].get_max_value
            for j in range(self.dropped[i]):
                self.histograms[i].record_value(max_value)

            # Get the total count of received requests
            total_count = self.histograms[i].get_total_count()

            # Get the 99th latency
            latency = self.histograms[i].get_value_at_percentile(99)

            # Prepare the json for output
            new_value = {
                'latency': latency,
                'per_core_through': (1.0 * (total_count - self.dropped[i]) /
                                     self.cores),
                'slo_success': 1.0 - (1.0 * self.violations[i] / total_count),
                'dropped_requests': self.dropped[i]
            }
            info.append(new_value)
        print json.dumps(info)

    def drop_request(self, flow_id):
        self.dropped[flow_id] += 1
        self.violations[flow_id] += 1
