import logging
from queue.request_queue import FIFORequestQueue
from scheduler.scheduler import CoreScheduler

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

        # One single global queue
        self.request_number = 0

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
