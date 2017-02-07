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
        # if (len(self.queue) > 0):
            # logging.debug('Scheduler: Can\'t schedule request {} Cores are'
                          # ' full at {}' % (request.idx, self.env.now))
            # yield self.env.timeout(0)



    # Start up if not already looping
    def become_active(self):
        logging.debug("CoreScheduler: Core {} becomes active".format(self.core_id))
        while not self.queue.empty():
            # Keep waiting for request

            req = self.queue.resource.request()

            # Waiting for my turn of the lock
            logging.debug("CoreScheduler: Core {} acquiring lock".format(self.core_id, self.env.now))
            yield req
            logging.debug("CoreScheduler: Core {} got lock at {}".format(self.core_id, self.env.now))

            request = self.queue.dequeue()
            if request is not None:
                self.env.process(self.process_request(request))

            # Can only dequeue once Each cycle
            yield self.env.timeout(self.queue.dequeue_time)
            self.queue.resource.release(req)


        logging.debug("CoreScheduler: Core {} becomes idle".format(self.core_id))
        self.host.core_become_idle(self)

