#!/usr/bin/python

import sys
import mat4py

from os import listdir
from os.path import join

if len(sys.argv) != 3:
    print "Usage: ./get_mat_file <path_to_results_folder> <output_file>"
    sys.exit(1)

files = listdir(sys.argv[1])
if len(files) !=  138:
    print "Not a valid results folder"
    sys.exit(1)

data = {}
data['load'] = [0.05 * i for i in range(1,19)]
data['load'] += [0.95, 0.96, 0.97, 0.98, 0.99]

data['latency'] = [0.0 for i in range(23)]

flow0_files = []
for fil in files:
    if fil[-11:] == "flow0.total":
        flow0_files += [fil]

for fil in flow0_files:
    load = float(fil.split("_")[10])
    if load > 0.95:
        index = 18 + int(round((load - 0.95) * 100))
    else:
        index = int(round(load / 0.05)) - 1
    with open(join(sys.argv[1], fil)) as f:
        data['latency'][index] = float(f.read())

mat4py.savemat(sys.argv[2], data)
