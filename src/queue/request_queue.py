import simpy
import logging
import collections


class RequestQueue(object):

    def __init__(self, env, size):
        self.env = env
        # If size = -1, assume infinite
        self.size = size


class FIFORequestQueue(RequestQueue):

    def __init__(self, env, size, dequeue_time):
        super(FIFORequestQueue, self).__init__(env, size)
        # TODO: If size is finite
        self.q = collections.deque()
        self.dequeue_time = dequeue_time

        # Assuming queue can only be accessed once at a time
        self.resource = simpy.Resource(env, capacity=1)

    def enqueue(self, request):
        self.q.append(request)

    def enqueue_front(self, request):
        self.q.appendleft(request)

    def empty(self):
        return len(self.q) == 0

    def dequeue(self):
        if len(self.q) == 0:
            return None
        return self.q.popleft()


class PerFlowRequestQueue(RequestQueue):

    def __init__(self, env, size, slo, load_ratio, num_cores):
        super(PerFlowRequestQueue, self).__init__(env, size)
        # TODO: If size is finite
        self.q = collections.deque()
        self.expected_length = 0.0
        self.slo = slo
        self.load_ratio = load_ratio
        self.num_cores = num_cores

    def enqueue(self, request):
        self.q.append(request)
        self.expected_length += request.expected_length
        return True

    def enqueue_front(self, request):
        self.q.appendleft(request)
        self.expected_length += request.expected_length

    def empty(self):
        return len(self.q) == 0

    def dequeue(self):
        if len(self.q) == 0:
            return None

        request = self.q.popleft()
        self.expected_length -= request.expected_length
        return request

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


class SLOFlowQueues(RequestQueue):

    def __init__(self, env, size, dequeue_time, flow_config, num_cores):
        super(SLOFlowQueues, self).__init__(env, size)
        # TODO: If size is finite
        self.q = []
        loads = [flow['load'] for flow in flow_config]
        load_ratios = [load / sum(loads) for load in loads]
        for flow in range(len(flow_config)):
            self.q.append(PerFlowRequestQueue(env, size,
                                              flow_config[flow]['slo'],
                                              load_ratios[flow], num_cores))
        self.dequeue_time = dequeue_time

        # Assume queues can only be accessed once at a time
        # FIXME Maybe change this to per queue lock
        self.resource = simpy.Resource(env, capacity=1)

    def enqueue(self, request):
        return self.q[request.flow_id].enqueue(request)

    def enqueue_front(self, request):
        self.q[request.flow_id].enqueue_front(request)

    def empty(self):
        # Check whether all the queues are empty
        for flow in self.q:
            if not flow.empty():
                return False

        return True

    def dequeue(self):
        if self.empty():
            return None

        flow = self.get_max_queue()
        return self.q[flow].dequeue()

    def get_max_queue(self):
        # Get the key of the flow with the longest queue
        max_value = 0
        max_index = 0
        for flow in range(len(self.q)):
            if self.q[flow].expected_length >= max_value:
                max_index = flow
                max_value = self.q[flow].expected_length
        logging.debug("Dequeuing request from flow {} with length {}".
                      format(max_index, max_value))
        return max_index


class SLOPerFlowQueues(SLOFlowQueues):

    def get_max_queue(self):
        # Get the key of the flow with the longest queue
        max_value = 0
        max_index = 0
        for flow in range(len(self.q)):
            if self.q[flow].get_load() >= max_value:
                max_index = flow
                max_value = self.q[flow].get_load()
        logging.debug("Dequeuing request from flow {} with length {}".
                      format(max_index, max_value))
        return max_index


class SLODropFlowQueues(SLOFlowQueues):

    def __init__(self, env, size, dequeue_time, flow_config, num_cores):
        self.env = env
        # TODO: If size is finite
        self.size = size
        self.q = []
        loads = [flow['load'] for flow in flow_config]
        load_ratios = [load / sum(loads) for load in loads]
        for flow in range(len(flow_config)):
            self.q.append(DropFlowRequestQueue(env, size,
                                               flow_config[flow]['slo'],
                                               load_ratios[flow], num_cores))
        self.dequeue_time = dequeue_time
        self.resource = simpy.Resource(env, capacity=1)

    def get_max_queue(self):
        # Get the key of the flow with the longest queue
        max_value = 0
        max_index = 0
        for flow in range(len(self.q)):
            if self.q[flow].get_load() >= max_value:
                max_index = flow
                max_value = self.q[flow].get_load()
        logging.debug("Dequeuing request from flow {} with length {}".
                      format(max_index, max_value))
        return max_index
