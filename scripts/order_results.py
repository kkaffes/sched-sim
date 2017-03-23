#!/usr/bin/env python

import sys

results = []
for line in sys.stdin:
    results.append(line)

new_results = []
new_results.append(results[3])   # 1 core
new_results.append(results[6])   # 2 cores
new_results.append(results[9])   # 3 cores
new_results.append(results[10])  # 4 cores
new_results.append(results[13])  # 5 cores
new_results.append(results[14])  # 6 cores
new_results.append(results[15])  # 7 cores
new_results.append(results[17])  # 8 cores
new_results.append(results[18])  # 9 cores
new_results.append(results[2])   # 10 cores
new_results.append(results[4])   # 15 cores
new_results.append(results[5])   # 20 cores
new_results.append(results[8])   # 25 cores
new_results.append(results[12])  # 50 cores
new_results.append(results[1])   # 100 cores
new_results.append(results[7])   # 250 cores
new_results.append(results[11])  # 500 cores
new_results.append(results[16])  # 750 cores
new_results.append(results[0])   # 1000 cores

for i in new_results:
    print str(i[:-1])
