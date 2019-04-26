import sys
import logging
from queue.request_queue import *
from scheduler.scheduler import *
from scheduler.load_balancer import LoadBalancer
from queue.dequeue_policy import *


class CoreGroup(object):

    idle_cores = []
    active_cores = []

    def __init__(self):
        self.idle_cores = []
        self.active_cores = []

    def pop_one_idle_core(self):
        # Return first idle core
        if len(self.idle_cores) != 0:
            return self.idle_cores.pop(0)
        else:
            return None

    def one_idle_core_become_active(self):
        a = self.pop_one_idle_core()
        if a:
            self.append_active_core(a)
        return a

    def available(self):
        return len(self.idle_cores) != 0

    def append_idle_core(self, core):
        self.idle_cores.append(core)

    def append_active_core(self, core):
        self.active_cores.append(core)

    def core_become_idle(self, core):
        self.active_cores.remove(core)
        self.idle_cores.append(core)

    def set_notifier(self, notifier):
        for core in self.idle_cores:
            # TODO: May need to check if cores are worker cores
            core.set_notifier(notifier)


class GlobalQueueHost(object):

    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):

        self.env = env
        self.core_group = CoreGroup()
        self.queue = FIFORequestQueue(env, -1, deq_cost, flow_config)

        for i in range(num_cores):
            new_core = CoreScheduler(env, histograms, i, flow_config)
            new_core.set_queue(self.queue)
            new_core.set_host(self)
            self.core_group.append_idle_core(new_core)

    def receive_request(self, request):
        logging.debug('Host: Received request %d from flow %d at %f' %
                      (request.idx, request.flow_id, self.env.now))

        self.queue.enqueue(request)

        # Putting active cores into list

        activate_core = self.core_group.pop_one_idle_core()
        if activate_core:
            self.env.process(activate_core.become_active())
            self.core_group.append_active_core(activate_core)

    def core_become_idle(self, core, done_request):
        self.core_group.core_become_idle(core)


class PartitionedGlobalQueueHost(object):

    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):

        self.env = env
        self.app_core_group = CoreGroup()
        self.net_core_group = CoreGroup()
        self.net_queue = FIFORequestQueue(env, -1, deq_cost, flow_config)
        self.app_queue = FIFORequestQueue(env, -1, deq_cost, flow_config)

        for i in range(num_cores):
            if i < int(opts.network_cores):
                new_core = NetworkCoreScheduler(env, histograms, i,
                                                flow_config)
                new_core.set_queue([self.net_queue, self.app_queue])
                new_core.set_host(self)
                self.net_core_group.append_idle_core(new_core)
            else:
                new_core = AppCoreScheduler(env, histograms, i, flow_config)
                new_core.set_queue(self.app_queue)
                new_core.set_host(self)
                self.app_core_group.append_idle_core(new_core)

    def receive_request(self, request):
        logging.debug('Host: Received request %d from flow %d at %f' %
                      (request.idx, request.flow_id, self.env.now))

        if request.network_time == 0.0:
            self.app_queue.enqueue(request)
            activate_core = self.app_core_group.pop_one_idle_core()
            if activate_core:
                self.env.process(activate_core.become_active())
                self.app_core_group.append_active_core(activate_core)
        else:
            self.net_queue.enqueue(request)
            activate_core = self.net_core_group.pop_one_idle_core()
            if activate_core:
                self.env.process(activate_core.become_active())
                self.net_core_group.append_active_core(activate_core)

    def core_become_idle(self, core, done_request, is_network):
        if is_network:
            self.net_core_group.core_become_idle(core)
        else:
            self.app_core_group.core_become_idle(core)


class MixedGlobalQueueHost(object):

    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):

        self.env = env
        self.core_group = CoreGroup()
        self.queue = FIFORequestQueue(env, -1, deq_cost, flow_config)

        for i in range(num_cores):
            new_core = MixedCoreScheduler(env, histograms, i, flow_config)
            new_core.set_queue(self.queue)
            new_core.set_host(self)
            self.core_group.append_idle_core(new_core)

    def receive_request(self, request):
        logging.debug('Host: Received request %d from flow %d at %f' %
                      (request.idx, request.flow_id, self.env.now))

        self.queue.enqueue(request)

        # Putting active cores into list

        activate_core = self.core_group.pop_one_idle_core()
        if activate_core:
            self.env.process(activate_core.become_active())
            self.core_group.append_active_core(activate_core)

    def core_become_idle(self, core, done_request):
        self.core_group.core_become_idle(core)


class MultiQueueHost(object):

    def __init__(self, env, num_queues, histograms, deq_cost, flow_config,
                 opts):
        self.env = env
        self.queues = []
        self.cores = []

        # Generate queues and cpus
        for i in range(num_queues):
            new_queue = FIFORequestQueue(env, -1, deq_cost, flow_config)
            new_core = CoreScheduler(env, histograms, i, flow_config)

            new_core.set_queue(new_queue)
            new_core.set_host(self)
            self.cores.append(new_core)
            self.queues.append(new_queue)

        self.load_balancer = LoadBalancer(num_queues)

    def receive_request(self, request):

        idx = self.load_balancer.queue_index_assign_to(request)

        logging.debug('Host: Received request {} at {}, assigning to queue {}'
                      .format(request.idx, self.env.now, idx))

        self.queues[idx].enqueue(request)
        self.env.process(self.cores[idx].become_active())

    def core_become_idle(self, core, done_request):
        pass


class ShinjukuHost(object):

    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):
        self.env = env
        self.queue = FIFORequestQueue(env, -1, deq_cost, flow_config)

        self.shinjuku = ShinjukuScheduler(env, histograms, flow_config)

        # Create core group and pass it to shinjuku scheduler
        new_cg = CoreGroup()

        # Make sure that there are at least 2 cores (dispatcher, worker)
        assert(num_cores > 1)

        for i in range(num_cores - 1):
            new_core = WorkerCore(env, histograms, i)
            new_cg.append_idle_core(new_core)

        new_cg.set_notifier(self.shinjuku)
        self.shinjuku.set_core_group(new_cg)
        self.shinjuku.set_queue(self.queue)

    def receive_request(self, request):
        logging.debug('Host: Received request {} at {}'.format(
            request.idx, self.env.now))

        self.queue.enqueue(request)

        # Wake up shinjuku
        self.env.process(self.shinjuku.become_active())


class PerFlowQueueHost(object):
    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):
        self.env = env
        self.core_group = CoreGroup()
        self.queues = None

        queue_policy = getattr(sys.modules[__name__], opts.queue_policy)
        loads = [flow['load'] for flow in flow_config]
        load_ratios = [load / sum(loads) for load in loads]

        # FIXME: if we want other per flow request queue type
        self.queues = PerFlowRequestQueueGroup(env, deq_cost, flow_config)
        for flow in range(len(flow_config)):
            self.queues.add_queue(PerFlowRequestQueue(env, -1,
                                              load_ratios[flow], num_cores,
                                              flow_config[flow]))

        self.dequeue_policy = queue_policy(env, self.queues)
        self.queues.set_dequeue_policy(self.dequeue_policy)

        self.histograms = histograms

        for i in range(num_cores):
            new_core = CoreScheduler(env, histograms, i, flow_config)
            new_core.set_queue(self.queues)
            new_core.set_host(self)
            self.core_group.append_idle_core(new_core)

    def receive_request(self, request):
        logging.debug('Host: Received request %d from flow %d at %f' %
                      (request.idx, request.flow_id, self.env.now))
        if not self.queues.enqueue(request):
            self.histograms.drop_request(request.flow_id)
            return

        # Putting active cores into list
        activate_core = self.core_group.pop_one_idle_core()
        if activate_core:
            self.env.process(activate_core.become_active())
            self.core_group.append_active_core(activate_core)

    def core_become_idle(self, core, done_request):
        self.core_group.core_become_idle(core)


class StaticCoreAllocationHost(object):

    def __init__(self, env, num_cores, histograms, deq_cost, flow_config,
                 opts):
        self.env = env
        self.core_groups = []
        self.queues = []

        loads = [flow['load'] for flow in flow_config]
        total_load = sum(loads)

        # For sanitity check
        total_cores = 0

        for i in range(len(flow_config)):
            new_queue = FIFORequestQueue(env, -1, deq_cost, flow_config)
            proportion = loads[i] / total_load
            num_core = int(round(num_cores * proportion))
            total_cores += num_core
            new_core_group = CoreGroup()
            for j in range(num_core):
                new_core = CoreScheduler(env, histograms, j, flow_config)
                new_core.set_queue(new_queue)
                new_core.set_host(self)
                new_core_group.append_idle_core(new_core)
            self.core_groups.append(new_core_group)
            self.queues.append(new_queue)

        if total_cores != num_cores:
            raise "Total number of cores not the same as num_cores in host.py"

    def receive_request(self, request):
        logging.debug('Host: Received request %d from flow %d at %f' %
                      (request.idx, request.flow_id, self.env.now))
        self.queues[request.flow_id - 1].enqueue(request)

        # Put active cores into list
        activate_core = self.core_groups[request.flow_id].\
            pop_one_idle_core()
        if activate_core:
            self.env.process(activate_core.become_active())
            self.core_groups[request.flow_id].\
                append_active_core(activate_core)

    def core_become_idle(self, core, done_request):
        self.core_groups[done_request.flow_id].core_become_idle(core)
