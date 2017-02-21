import sys

results = []
for line in sys.stdin:
	results.append(line)

new_results = []
new_results.append(results[2]) # std_dev 1
new_results.append(results[5]) # std_dev 1
new_results.append(results[8]) # std_dev 1
new_results.append(results[9]) # std_dev 1
new_results.append(results[11]) # std_dev 1
new_results.append(results[12]) # std_dev 1
new_results.append(results[13]) # std_dev 1
new_results.append(results[14]) # std_dev 1
new_results.append(results[15]) # std_dev 1
new_results.append(results[1]) # std_dev 1
new_results.append(results[3]) # std_dev 1
new_results.append(results[4]) # std_dev 1
new_results.append(results[7]) # std_dev 1
new_results.append(results[10]) # std_dev 1
new_results.append(results[0]) # std_dev 1
new_results.append(results[6]) # std_dev 1

for i in new_results:
    print str(i[:-1])
