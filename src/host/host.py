import logging
from queue.request_queue import FIFORequestQueue
from scheduler.scheduler import CoreScheduler
from scheduler.load_balancer import LoadBalancer

class GlobalQueueHost(object):
    def __init__(self, env, num_cores, histogram):
        self.env = env
        self.idle_cores = []
        self.active_cores = []
        self.queue = FIFORequestQueue(env, -1, 1)

        for i in range(num_cores):
            new_core = CoreScheduler(env, histogram, i)
            new_core.set_queue(self.queue)
            new_core.set_host(self)
            self.idle_cores.append(new_core)


    def receive_request(self, request):
        logging.debug('Host: Received request %d at %d' % (request.idx,
                      self.env.now))

        self.queue.enqueue(request)
        for i in self.idle_cores:
            self.env.process(i.become_active())

        # Putting active cores into list
        while len(self.idle_cores) != 0:
            self.active_cores.append(self.idle_cores.pop(0))

    def core_become_idle(self, core):
        self.active_cores.remove(core)
        self.idle_cores.append(core)

class MultiQueueHost(object):

    idle_cores = []

    def __init__(self, env, num_queues, histogram):
        self.env = env

        self.queues = []
        self.cores = []
        # Generate queues and cpus
        for i in range(num_queues):
            new_queue = FIFORequestQueue(env, -1, 1)
            new_core = CoreScheduler(env, histogram, i)

            new_core.set_queue(new_queue)
            new_core.set_host(self)
            self.cores.append(new_core)
            self.queues.append(new_queue)

        self.load_balancer = LoadBalancer(num_queues)


    def receive_request(self, request):

        idx = self.load_balancer.queue_index_assign_to(request)

        logging.debug('Host: Received request {} at {}, assigning to queue {}'.format(request.idx,
                      self.env.now, idx))

        self.queues[idx].enqueue(request)

        self.env.process(self.cores[idx].become_active())

    def core_become_idle(self, core):
        pass
