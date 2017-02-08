

class Request(object):
    def __init__(self, idx, exec_time, start_time, flow_id):
        self.idx = idx
        self.exec_time = exec_time
        self.start_time = start_time
        self.flow_id = flow_id
