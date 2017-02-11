import random
from request import Request


class RequestGenerator(object):

    flow_id = 1

    def __init__(self, env, host, load, num_cores):
        self.env = env
        self.host = host
        self.load = load
        self.num_cores = num_cores

    def set_host(self, host):
        self.host = host

    def begin_generation(self):
        self.action = self.env.process(self.run())

    def set_flow_id(self, flow_id):
        self.flow_id = flow_id

    def run(self):
        idx = 0
        while True:
            # Fixed 7 execution time
            self.host.receive_request(Request(idx, 7,
                                              self.env.now,
                                              self.flow_id))
            yield self.env.timeout(1)
            idx += 1


class MultipleRequestGenerator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.generators = []
        self.cur_flow_id = 1
        self.idx = 0

    def add_generator(self, gen):
        gen.set_flow_id(self.cur_flow_id)
        self.cur_flow_id += 1
        self.generators.append(gen)

    def begin_generation(self):
        for i in self.generators:
            i.set_host(self)
            i.begin_generation()

    def receive_request(self, request):
        request.idx = self.idx
        self.host.receive_request(request)
        self.idx += 1



class HeavyTailRequestGenerator(RequestGenerator):
    def __init__(self, env, host, inter_gen, num_cores, opts):
        # Tail percent of 2 means that 2% of requests require "tail latency"
        # execution time, the others require "latency" execution
        # time.
        RequestGenerator.__init__(self, env, host, opts["load"], num_cores)
        self.exec_time = opts["exec_time"]
        self.heavy_exec_time = opts["heavy_time"]
        self.heavy_percent = opts["heavy_per"]

        inv_load = 1.0 / self.load
        mean = (inv_load * (self.heavy_exec_time * (self.heavy_percent /
                100.0) + self.exec_time * ((100 - self.heavy_percent) /
                100.0)) / self.num_cores)
        self.inter_gen = inter_gen(mean, opts["std_dev"])

    def run(self):
        idx = 0
        while True:
            # Generate inter-arrival time
            s = self.inter_gen.next()
            yield self.env.timeout(s)

            # Generate request
            # NOTE Percentage must be integer
            is_heavy = random.randint(0, 99) < self.heavy_percent
            exec_time = self.heavy_exec_time if is_heavy else self.exec_time
            self.host.receive_request(Request(idx,
                                              exec_time,
                                              self.env.now, self.flow_id))

            idx += 1
