import logging

class LoadBalancer(object):

    index = 0
    def __init__(self, num_queues):
        self.num_queues = num_queues
        pass

    def queue_index_assign_to(self, request):
        # Right now we're only doing round robin,
        # so the request field is not used
        to_return = self.index
        self.index = (self.index + 1) % self.num_queues
        return to_return
