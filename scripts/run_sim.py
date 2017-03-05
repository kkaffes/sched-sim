#!/usr/bin/env python

import os
import sys
import copy
import json
import time
import tempfile
import subprocess

from multiprocessing import Process

OUTPUT_DIR = "../out/"


def main():
    # Set the simulation parameters
    iterations = 20
    core_count = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 50, 75, 100]
    core_count = [1]
    host_types = ["perflow"]
    deq_costs = [0.0]
    queue_policies = ['SLOFlowQueues']

    batch_run = 5

    config_jsons = [[{
                        "work_gen": "heavy_tail",
                        "inter_gen": "poisson_arrival",
                        "load": 0.4,
                        "exec_time": 20.0,
                        "heavy_per": 2,
                        "heavy_time": 20.0,
                        "time_slice": 0.0,
                        "slo": 30.0
                     },
                     {
                        "work_gen": "heavy_tail",
                        "inter_gen": "poisson_arrival",
                        "load": 0.4,
                        "exec_time": 100.0,
                        "heavy_per": 2,
                        "heavy_time": 100.0,
                        "time_slice": 0.0,
                        "slo": 150.0
                     }]]

    # for i in range(2,10):
    #    temp_conf = copy.deepcopy(config_jsons[0])
    #    temp_conf[1]["exec_time"] = i * 2.0
    #    temp_conf[1]["heavy_time"] = i * 2.0
    #    config_jsons.append(temp_conf)

    # for i in range(1,11):
    #    temp_conf = copy.deepcopy(config_jsons[0])
    #    temp_conf[1]["exec_time"] = i * 20.0
    #    temp_conf[1]["heavy_time"] = i * 20.0
    #    config_jsons.append(temp_conf)

    idle = []
    running = []
    for deq_cost in deq_costs:
        for host in host_types:
            for cores in core_count:
                for config_json in config_jsons:
                    for queue_policy in queue_policies:
                        p = Process(target=run_sim, args=(deq_cost, host,
                                                          cores, config_json,
                                                          queue_policy,
                                                          iterations))
                        idle.append(p)

    # Running phase
    while len(idle) > 0:
        while len(running) < batch_run and len(idle) > 0:
            p = idle.pop(0)
            p.start()
            running.append(p)
        to_finish = []
        for process in running:
            if not process.is_alive():
                to_finish.append(process)
        for p in to_finish:
            running.remove(p)
        time.sleep(1)

    print "Winding down"

    # Wind down phase
    for run in running:
        run.join()


def run_sim(deq_cost, host, cores, config_json, queue_policy, iterations):
    # Create config file
    conf, config_file = tempfile.mkstemp()
    os.write(conf, json.dumps(config_json))
    os.close(conf)

    # Run the simulation
    sim_args = ["../src/sim.py",
                "--cores", str(cores),
                "--workload-conf", str(config_file),
                "--host-type", str(host),
                "--deq-cost", str(deq_cost),
                "--queue-policy", queue_policy]

    per_flow_throughput = []
    per_flow_latency = []
    per_flow_slo = []
    for i in range(len(config_json)):
        per_flow_latency.append([])
        per_flow_throughput.append([])
        per_flow_slo.append([])

    running_jobs = []
    for i in range(iterations):
        p = subprocess.Popen(sim_args, stdout=subprocess.PIPE)
        running_jobs.append(p)

    for p in running_jobs:
        out, err = p.communicate()
        output = json.loads(out)
        for i in range(len(config_json)):
            per_flow_latency[i].append(output[i]['latency'])
            per_flow_throughput[i].append(output[i]['per_core_through'])
            per_flow_slo[i].append(output[i]['slo_success'])

    # Gather the results
    output_name = (OUTPUT_DIR + "sim_" + str(cores) + "_" + str(host) + "_" +
                   str(deq_cost) + "_" + queue_policy)
    full_name = output_name
    for key in range(len(config_json)):
        val = config_json[key]
        flow_name = ("_" + "flow" + str(key) + "_" + str(val["work_gen"]) +
                     "_" + str(val["inter_gen"]) + "_" + str(val["load"]))

        if val.get("mean"):
            flow_name += "_" + str(val["mean"])
        if val.get("std_dev_request"):
            flow_name += "_" + str(val["std_dev_request"])
        if val.get("exec_time"):
            flow_name += "_" + str(val["exec_time"])
        if val.get("heavy_per"):
            flow_name += "_" + str(val["heavy_per"])
        if val.get("heavy_time"):
            flow_name += "_" + str(val["heavy_time"])
        if val.get("std_dev_arrival"):
            flow_name += "_" + str(val["std_dev_arrival"])
        if val.get("time_slice"):
            flow_name += "_" + str(val["time_slice"])
        if val.get("enq_front"):
            flow_name += "_enqfront" + str(val["enq_front"])

        full_name += flow_name

    for i in range(len(config_json)):
        flow_name = "_" + "flow" + str(i)
        flow_name = full_name + flow_name
        with open(flow_name, 'w') as f:
            for value in per_flow_latency[i]:
                f.write(str(value) + "\n")

        flow_name = flow_name + ".total"
        with open(flow_name, 'w') as f:
            value = sum(per_flow_latency[i]) * 1.0 / len(per_flow_latency[i])
            f.write(str(value) + "\n")

        flow_name = "_" + "flow" + str(i)
        flow_name = full_name + flow_name + '.throughput'
        with open(flow_name, 'w') as f:
            for value in per_flow_throughput[i]:
                f.write(str(value) + "\n")

        flow_name = flow_name + ".total"
        with open(flow_name, 'w') as f:
            value = (sum(per_flow_throughput[i]) * 1.0 /
                     len(per_flow_throughput[i]))
            f.write(str(value) + "\n")

        flow_name = "_" + "flow" + str(i)
        flow_name = full_name + flow_name + '.slo'
        with open(flow_name, 'w') as f:
            for value in per_flow_slo[i]:
                f.write(str(value) + "\n")

        flow_name = flow_name + ".total"
        with open(flow_name, 'w') as f:
            value = (sum(per_flow_slo[i]) * 1.0 /
                     len(per_flow_slo[i]))
            f.write(str(value) + "\n")

    # Delete config file
    try:
        os.remove(config_file)
    except:
        pass


if __name__ == "__main__":
    main()
