import simpy
import Queue
import logging


class RequestQueue(object):

    def __init__(self, env, size):
        self.env = env
        # If size = -1, assume infinite
        self.size = size


class FIFORequestQueue(RequestQueue):

    def __init__(self, env, size, dequeue_time):
        super(FIFORequestQueue, self).__init__(env, size)
        # TODO: If size is finite
        self.q = Queue.Queue()
        self.dequeue_time = dequeue_time

        # Assuming queue can only be accessed once at a time
        self.resource = simpy.Resource(env, capacity=1)

    def enqueue(self, request):
        self.q.put(request)

    def empty(self):
        return self.q.empty()

    def dequeue(self):
        if self.q.empty():
            return None

        return self.q.get()
