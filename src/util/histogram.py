#!/usr/bin/env python
from hdrh.histogram import HdrHistogram

class Histogram(object):

    def __init__(self, num_histograms):
        self.histograms = [HdrHistogram(1, 1000 * 1000, 2)
                           for i in range(num_histograms + 1)]

    def record_value(self, flow, value):
        self.histograms[0].record_value(value)
        self.histograms[flow].record_value(value)

    def print_percentile(self, percentile):
        for i in self.histograms:
            print i.get_value_at_percentile(percentile)
