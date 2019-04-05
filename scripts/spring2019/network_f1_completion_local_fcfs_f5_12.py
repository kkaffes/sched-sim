#!/usr/bin/env python

import os
import copy
import json
import time
import math
import random
import tempfile
import subprocess

from multiprocessing import Process

OUTPUT_DIR = "../../out/spring2019/network_f1_completion_local_fcfs_f5_12/"

def main():
    global OUTPUT_DIR
    # Set the simulation parameters
    iterations = 10
    core_count = [12]
    host_types = ['local']
    deq_costs = [0.0]
    queue_policies = ['global']

    cores_to_run = 24
    batch_run = math.ceil(float(cores_to_run) / iterations)

    # Sanity check
    if not OUTPUT_DIR.endswith("/"):
        OUTPUT_DIR += "/"

    # Create folder if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    config_jsons = []
    default_json = [{
        "work_gen": "network_request",
        "inter_gen": "poisson_arrival",
        "exec_time": 5.0,
        #"mean": 5.0,
        #"std_dev_request": 1.0,
        "network_time": 1.0,
        #"network_mean": 1.0,
        #"std_dev_network": 1.0,
        "load": 0.9,
        "time_slice": 0.0,
        "enq_front": False
        }]

    loads = [0.05 * i for i in range(1,19)]
    loads += [0.95, 0.96, 0.97, 0.98, 0.99]
    for i in loads:
        temp_conf = copy.deepcopy(default_json)
        temp_conf[0]["load"] = i
        config_jsons.append(temp_conf)

    seeds = [497577696, 308484504, 976250624, 331509278, 373072862, 494155711,
             64603035, 414537690, 712438709, 566941566, 356444130, 198904022,
             906581464, 44964761, 931163827, 797805274, 344646089, 387905473,
             298058383, 664246766]

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
                                                          iterations,
                                                          seeds))
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


def run_sim(deq_cost, host, cores, config_json, queue_policy,
            iterations, seeds):
    # Create config file
    conf, config_file = tempfile.mkstemp()
    os.write(conf, json.dumps(config_json))
    os.close(conf)

    # Run the simulation
    sim_args = ["../../src/sim.py",
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
        arg = copy.deepcopy(sim_args)
        arg.extend(["-s", str(seeds[i])])
        p = subprocess.Popen(arg, stdout=subprocess.PIPE)
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

        if val.get("mean") is not None:
            flow_name += "_" + str(val["mean"])
        if val.get("std_dev_request") is not None:
            flow_name += "_" + str(val["std_dev_request"])
        if val.get("exec_time") is not None:
            flow_name += "_" + str(val["exec_time"])
        if val.get("heavy_per") is not None:
            flow_name += "_" + str(val["heavy_per"])
        if val.get("heavy_time") is not None:
            flow_name += "_" + str(val["heavy_time"])
        if val.get("std_dev_arrival") is not None:
            flow_name += "_" + str(val["std_dev_arrival"])
        if val.get("time_slice") is not None:
            flow_name += "_" + str(val["time_slice"])
        if val.get("enq_front") is not None:
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
