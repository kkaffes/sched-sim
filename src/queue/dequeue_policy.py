import logging

class GlobalDequeuePolicy(object):

    queues = None
    def __init__(self, env, queues):
        self.env = env
        self.queues = queues

class SelectDequeuePolicy(GlobalDequeuePolicy):
    def __init__(self, env, queues):
        super(SelectDequeuePolicy, self).__init__(env, queues)

    def select_queue(self):
        raise ("SelectDequeuePolicy's select shouldn't be called")

    def dequeue(self):
        if self.queues.empty():
            return None

        flow = self.select_queue()
        return self.queues[flow].dequeue()

class LongestLengthDequeuePolicy(SelectDequeuePolicy):
    def select_queue(self):
        # Get the key of the flow with the longest queue
        max_value = 0
        max_index = 0
        for flow in range(len(self.queues)):
            if self.queues[flow].expected_length >= max_value:
                max_index = flow
                max_value = self.queues[flow].expected_length
        logging.debug("Dequeuing request from flow {} with length {}".
                      format(max_index, max_value))
        return max_index


class LongestLoadDequeuePolicy(SelectDequeuePolicy):
    def select_queue(self):
        # Get the key of the queue whose first packet is closest to violating
        # its SLO.
        max_value = 0
        max_index = 0
        for flow in range(len(self.queues)):
            if self.queues[flow].get_load() >= max_value:
                max_index = flow
                max_value = self.queues[flow].get_load()
        logging.debug("Dequeuing request from flow {} with length {}".
                      format(max_index, max_value))
        return max_index

class FirstPacketLatencyDequeuePolicy(SelectDequeuePolicy):
    def select_queue(self):
        # Get the key of the queue whose first packet is closest to violating
        # its SLO.
        max_value = 0
        max_index = 0
        for flow in range(len(self.queues)):
            if self.queues[flow].get_first_packet_latency() >= max_value:
                max_index = flow
                max_value = self.queues[flow].get_first_packet_latency()
        logging.debug("Dequeuing request from flow {} with first packet slo"
                      " metric {}". format(max_index, max_value))
        return max_index

class FirstPacketWaitDequeuePolicy(SelectDequeuePolicy):
    def select_queue(self):
        # Get the key of the queue whose first packet is closest to violating
        # its SLO knowing only its wait time.
        max_value = 0
        max_index = 0
        for flow in range(len(self.queues)):
            if self.queues[flow].get_first_packet_wait() >= max_value:
                max_index = flow
                max_value = self.queues[flow].get_first_packet_wait()
        logging.debug("Dequeuing request from flow {} with first packet wait"
                      " metric {}". format(max_index, max_value))
        return max_index

class RoundRobinDequeuePolicy(SelectDequeuePolicy):
    current_q = -1
    num_queues = -1

    def __init__(self, env, queues):
        super(RoundRobinDequeuePolicy, self).__init__(env, queues)
        self.current_q = 0
        self.num_queues = len(queues)

    def select_queue(self):

        choose = self.current_q
        if self.current_q == self.num_queues - 1:
            self.current_q = 0
        else: self.current_q += 1

        # Making sure that it's not dequeuing from wrong queue
        while self.queues[choose].empty():
            choose = self.current_q
            if self.current_q == self.num_queues - 1:
                self.current_q = 0
            else: self.current_q += 1

        logging.debug("Dequeuing request from flow {} "
                      "with Round Robin".format(choose))
        return choose
