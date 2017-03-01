import logging
from queue.request_queue import FIFORequestQueue, FlowQueues
from scheduler.scheduler import *
from scheduler.load_balancer import LoadBalancer


class CoreGroup(object):

    idle_cores = []
    active_cores = []

    def __init__(self):
        self.idle_cores = []
        self.active_cores = []

    def pop_one_idle_core(self):
        # just return first idle core
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

    def __init__(self, env, num_cores, deq_cost, time_slice, histograms,
                 num_flows):
        self.env = env
        self.core_group = CoreGroup()
        self.queue = FIFORequestQueue(env, -1, deq_cost)

        for i in range(num_cores):
            new_core = CoreScheduler(env, histograms, i, time_slice)
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

    def core_become_idle(self, core):
        self.core_group.core_become_idle(core)


class MultiQueueHost(object):

    def __init__(self, env, num_queues, deq_cost, time_slice, histograms,
                 num_flows):
        self.env = env

        self.queues = []
        self.cores = []
        # Generate queues and cpus
        for i in range(num_queues):
            new_queue = FIFORequestQueue(env, -1, deq_cost)
            new_core = CoreScheduler(env, histograms, i, time_slice)

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

    def core_become_idle(self, core):
        pass


class ShinjukuHost(object):

    def __init__(self, env, num_cores, deq_cost, time_slice, histograms,
                 num_flows):
        self.env = env
        self.queue = FIFORequestQueue(env, -1, deq_cost)

        self.shinjuku = ShinjukuScheduler(env, histograms, time_slice)

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
    def __init__(self, env, num_cores, deq_cost, time_slice, histograms,
                 num_flows):
        self.env = env
        self.core_group = CoreGroup()
        self.queue = FlowQueues(env, -1, deq_cost, num_flows)

        for i in range(num_cores):
            new_core = CoreScheduler(env, histograms, i, time_slice)
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

    def core_become_idle(self, core):
        self.core_group.core_become_idle(core)
