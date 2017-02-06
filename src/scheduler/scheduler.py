import logging

class Scheduler(object):
    def __init__(self, env, histogram):
        self.env = env
        self.histogram = histogram
        self.queue = []
        self.request_number = 0

    def set_host(self, host):
        self.host = host

    def find_empty_cores(self):
        for i in range(len(self.host.cores)):
            if not self.host.cores[i]:
                return i
        return None

    def handle_request(self, request):
        self.queue.append(request)
        empty_core = self.find_empty_cores()

        while ((empty_core is not None) and len(self.queue) > 0):
            request = self.queue.pop(0)
            logging.debug('Scheduler: Assigning request %d to core %d at %d'
                          % (request.idx, empty_core, self.env.now))
            self.host.cores[empty_core] = request
            yield self.env.timeout(request.exec_time)
            self.host.cores[empty_core] = None
            latency = self.env.now - request.start_time
            logging.debug('Scheduler: Request %d Latency %d' %
                          (request.idx, latency))
            self.histogram.record_value(latency)
            logging.debug('Scheduler: Request %d finished execution at core %d'
                          ' at %d' % (request.idx, empty_core, self.env.now))
            self.request_number = self.request_number + 1
            empty_core = self.find_empty_cores()
        if (len(self.queue) > 0):
            logging.debug('Scheduler: Can\'t schedule request %d Cores are'
                          ' full at %d' % (request.idx, self.env.now))
            yield self.env.timeout(0)
