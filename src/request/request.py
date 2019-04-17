

class Request(object):
    def __init__(self, idx, exec_time, start_time, flow_id, expected_length):
        self.idx = idx
        self.network_time = 0
        self.exec_time = exec_time
        self.start_time = start_time
        self.flow_id = flow_id
        self.expected_length = exec_time

    def __init__(self, idx, exec_time, network_time, start_time, flow_id,
                 expected_length=0):
        self.idx = idx
        self.network_time = network_time
        self.exec_time = exec_time
        self.start_time = start_time
        self.flow_id = flow_id
        self.expected_length = exec_time
