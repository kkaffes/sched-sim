import logging

class Host(object):
    def __init__(self, env, sched, num_cores):
        self.env = env
        self.cores = [None] * num_cores
        self.sched = sched
        self.sched.set_host(self)

    def receive_request(self, request):
        logging.debug('Host: Received request %d at %d' % (request.idx,
                      self.env.now))
        self.env.process(self.sched.handle_request(request))
