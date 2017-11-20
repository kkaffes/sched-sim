import simpy
import logging
import collections

class RequestQueue(object):

    def __init__(self, env, size):
        self.env = env
        # If size = -1, assume infinite
        self.size = size


class FIFORequestQueue(RequestQueue):

    def __init__(self, env, size, dequeue_time, flow_config):
        super(FIFORequestQueue, self).__init__(env, size)
        # TODO: If size is finite
        self.q = collections.deque()
        self.dequeue_time = dequeue_time

        #Passing in this just so match the previous version
        self.flow_config = flow_config

        # Assuming queue can only be accessed once at a time
        self.resource = simpy.Resource(env, capacity=1)

    def enqueue(self, request):
        self.q.append(request)

    def enqueue_front(self, request):
        self.q.appendleft(request)

    def renqueue(self, request):
        if (self.flow_config[request.flow_id]['enq_front']):
            self.enqueue_front(request)
        else:
            self.enqueue(request)


    def empty(self):
        return len(self.q) == 0

    def dequeue(self):
        if len(self.q) == 0:
            return None
        return self.q.popleft()


class PerFlowRequestQueue(RequestQueue):

    def __init__(self, env, size, load_ratio, num_cores,
                 flow_config):
        super(PerFlowRequestQueue, self).__init__(env, size)
        # TODO: If size is finite
        self.q = collections.deque()
        self.expected_length = 0.0
        self.load_ratio = load_ratio
        self.num_cores = num_cores
        self.flow_config = flow_config
        self.slo = self.flow_config["slo"]
        self.enq_front = self.flow_config['enq_front']

    def enqueue(self, request):
        self.q.append(request)
        self.expected_length += request.expected_length
        return True

    def enqueue_front(self, request):
        self.q.appendleft(request)
        self.expected_length += request.expected_length

    def renqueue(self, request):
        if self.enq_front:
            self.enqueue_front(request)
        else:
            self.enqueue(request)

    def empty(self):
        return len(self.q) == 0

    def dequeue(self):
        if self.empty():
            return None

        request = self.q.popleft()
        self.expected_length -= request.expected_length
        return request

    def get_first_packet_latency(self):
        if self.empty():
            return 0

        request = self.q[0]
        return ((self.env.now - request.start_time + request.exec_time) /
                self.slo)

    def get_first_packet_wait(self):
        if self.empty():
            return -1

        request = self.q[0]
        return ((self.env.now - request.start_time) /
                self.slo)

    def get_load(self):
        if self.empty():
            return 0
        else:
            return (self.expected_length / self.slo / (self.load_ratio *
                                                       self.num_cores))

class DropFlowRequestQueue(PerFlowRequestQueue):

    def enqueue(self, request):
        self.expected_length += request.expected_length
        if self.get_load() > 1.0:
            self.expected_length -= request.expected_length
            return False
        else:
            self.q.append(request)
            return True

class PerFlowRequestQueueGroup(RequestQueue):
    qs = []

    def __init__(self, env, dequeue_time, flow_config):
        super(PerFlowRequestQueueGroup, self).__init__(env, len(flow_config))
        self.dequeue_time = dequeue_time
        self.flow_config = flow_config
        # Assuming queue can only be accessed once at a time
        self.resource = simpy.Resource(env, capacity=1)

    def set_dequeue_policy(self, dqp):
        self.dequeue_policy = dqp

    def enqueue(self, request):
        return self.qs[request.flow_id].enqueue(request)

    def renqueue(self, request):
        return self.qs[request.flow_id].renqueue(request)

    def add_queue(self, q):
        self.qs.append(q)

    def dequeue(self):
        return self.dequeue_policy.dequeue()

    def empty(self):
        for flow in self.qs:
            if not flow.empty():
                return False
        return True

    def __getitem__(self, key):
        return self.qs[key]

    def __len__(self):
        return len(self.qs)

