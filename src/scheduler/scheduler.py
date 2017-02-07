import logging


class CoreScheduler(object):
    def __init__(self, env, histogram, core_id):
        self.env = env
        self.histogram = histogram
        self.core_id = core_id
        self.active = False

    def set_queue(self, queue):
        self.queue = queue

    def set_host(self, host):
        self.host = host

    def active(self):
        return self.active

    def process_request(self, request):
        logging.debug('Scheduler: Assigning request {} to core {} at {}'
                      .format(request.idx, self.core_id, self.env.now))
        yield self.env.timeout(request.exec_time)
        latency = self.env.now - request.start_time
        logging.debug('Scheduler: Request {} Latency {}'.format
                      (request.idx, latency))
        self.histogram.record_value(latency)
        logging.debug('Scheduler: Request {} finished execution at core {}'
                      ' at {}'.format(request.idx, self.core_id, self.env.now))

    # Start up if not already looping
    def become_active(self):
        if (self.active):
            return

        # Makes sure it goes idle only afterprocess finishes
        self.active = True
        logging.debug("CoreScheduler: Core {} becomes active at {}"
                      .format(self.core_id, self.env.now))
        while not self.queue.empty():
            # Keep waiting for request

            req = self.queue.resource.request()

            # Waiting for my turn of the lock
            logging.debug("CoreScheduler: Core {} acquiring lock"
                          .format(self.core_id, self.env.now))
            yield req
            logging.debug("CoreScheduler: Core {} got lock at {}"
                          .format(self.core_id, self.env.now))

            request = self.queue.dequeue()

            p = None
            if request is not None:
                p = self.env.process(self.process_request(request))
                # Can only dequeue once Each cycle
                yield self.env.timeout(self.queue.dequeue_time)

            self.queue.resource.release(req)

            if p:
                yield p

        logging.debug("CoreScheduler: Core {} becomes idle at {}"
                      .format(self.core_id, self.env.now))
        self.active = False

        if self.host:
            self.host.core_become_idle(self)
