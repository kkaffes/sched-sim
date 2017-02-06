
class Request(object):
    def __init__(self, idx, exec_time, start_time):
        self.idx = idx
        self.exec_time = exec_time
        self.start_time = start_time


class RequestGenerator(object):
    def __init__(self, env, host):
        self.env = env
        self.host = host
        self.action = env.process(self.run())

    def run(self):
        idx = 0
        while True:
            self.host.receive_request(Request(idx, 7, self.env.now))
            yield self.env.timeout(1)
            idx += 1

