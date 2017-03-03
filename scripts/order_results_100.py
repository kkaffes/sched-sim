#!/usr/bin/env python

import sys

results = []
for line in sys.stdin:
    results.append(line)

new_results = []
new_results.append(results[2]) # 1 core
new_results.append(results[5]) # 2 cores
new_results.append(results[7]) # 3 cores
new_results.append(results[8]) # 4 cores
new_results.append(results[10]) # 5 cores
new_results.append(results[11]) # 6 cores
new_results.append(results[12]) # 7 cores
new_results.append(results[14]) # 8 cores
new_results.append(results[15]) # 9 cores
new_results.append(results[1]) # 10 cores
new_results.append(results[3]) # 15 cores
new_results.append(results[4]) # 20 cores
new_results.append(results[6]) # 25 cores
new_results.append(results[9]) # 50 cores
new_results.append(results[13]) # 75 cores
new_results.append(results[0]) # 100 cores

for i in new_results:
    print str(i[:-1])
