#!/usr/bin/env python

import json
from hdrh.histogram import HdrHistogram


class Histogram(object):

    def __init__(self, num_histograms, cores, flow_config):
        self.histograms = [HdrHistogram(1, 1000 * 1000, 2)
                           for i in range(num_histograms)]
        self.global_histogram = HdrHistogram(1, 1000 * 1000, 2)
        self.cores = cores
	self.flow_config = flow_config
	self.violations = [0 for i in range(len(flow_config))]

    def record_value(self, flow, value):
        self.global_histogram.record_value(value)
        self.histograms[flow].record_value(value)
	if self.flow_config[flow].get('slo'):
	    if value > self.flow_config[flow].get('slo'):
	        self.violations[flow] += 1

    def print_info(self):
        info = []
        for i in range(len(self.histograms)):
            total_count = self.histograms[i].get_total_count()
            latency = self.histograms[i].get_value_at_percentile(99)
            new_value = {
                'latency': latency,
                'per_core_through': 1.0 * total_count / self.cores,
                'slo_success': 1.0 - (1.0 * self.violations[i] / total_count)
            }
            info.append(new_value)
        print json.dumps(info)
