#!/usr/bin/env python
from hdrh.histogram import HdrHistogram


class Histogram(object):

    def __init__(self, num_histograms, cores):
        self.histograms = [HdrHistogram(1, 1000 * 1000, 2)
                           for i in range(num_histograms + 1)]
        self.cores = cores

    def record_value(self, flow, value):
        self.histograms[0].record_value(value)
        self.histograms[flow].record_value(value)

    def print_percentile(self, percentile):
        for i in self.histograms:
            print i.get_value_at_percentile(percentile)

    def print_total_count(self):
        for i in self.histograms:
            print i.get_total_count()

    def print_per_core_count(self):
        print self.histograms[0].get_total_count() / self.cores
