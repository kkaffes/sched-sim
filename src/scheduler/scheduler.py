import logging


class CoreScheduler(object):
    def __init__(self, env, histograms, core_id, time_slice):
        self.env = env
        self.histograms = histograms
        self.core_id = core_id
        self.time_slice = time_slice
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
        if (self.time_slice == 0 or self.time_slice > request.exec_time):
            yield self.env.timeout(request.exec_time)
            latency = self.env.now - request.start_time
            logging.debug('Scheduler: Request {} Latency {}'.format
                          (request.idx, latency))
            flow_id = request.flow_id
            self.histograms[flow_id].record_value(latency)
            logging.debug('Scheduler: Request {} finished execution at core {}'
                          ' at {}'.format(request.idx, self.core_id,
                                          self.env.now))
        else:
            yield self.env.timeout(self.time_slice)
            request.exec_time -= self.time_slice
            logging.debug('Scheduler: Request {} preempted at core {} at {}'
                          .format(request.idx, self.core_id, self.env.now))
            # FIXME Add enqueue cost/lock
            # Add the unfinished request to the queue
            self.queue.enqueue(request)

    # Start up if not already looping
    def become_active(self):
        if (self.active):
            return

        # Become idle only after process finishes
        self.active = True
        logging.debug("CoreScheduler: Core {} becomes active at {}"
                      .format(self.core_id, self.env.now))
        while not self.queue.empty():
            # Keep waiting for request
            req = self.queue.resource.request()

            # Wait for my turn of the lock
            logging.debug("CoreScheduler: Core {} acquiring lock"
                          .format(self.core_id, self.env.now))
            yield req
            logging.debug("CoreScheduler: Core {} got lock at {}"
                          .format(self.core_id, self.env.now))

            request = self.queue.dequeue()

            p = None
            if request is not None:
                p = self.env.process(self.process_request(request))
                # Can only dequeue once each cycle
                yield self.env.timeout(self.queue.dequeue_time)

            self.queue.resource.release(req)

            if p:
                yield p

        logging.debug("CoreScheduler: Core {} becomes idle at {}"
                      .format(self.core_id, self.env.now))
        self.active = False

        if self.host:
            self.host.core_become_idle(self)
