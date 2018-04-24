import logging


class ShinjukuScheduler(object):
    # This is for if we want to add request to another queue
    output_queues = []

    def __init__(self, env, histograms, flow_config):
        self.env = env
        self.histograms = histograms
        self.flow_config = flow_config
        self.active = False

    def set_core_group(self, group):
        self.core_group = group

    def append_output_queue(self, queue):
        self.output_queues.append(queue)

    def set_queue(self, queue):
        self.queue = queue

    def become_active(self):
        if (self.active):
            return
        self.active = True

        logging.debug("Shinjuku: Becomes active at {}".format(self.env.now))
        while not self.queue.empty() and self.core_group.available():
            request = self.queue.dequeue()
            yield self.env.timeout(self.queue.dequeue_time)

            # Calculate how much time to run
            time_slice = self.flow_config[request.flow_id].get("time_slice")
            if time_slice:
                run_time = (time_slice if time_slice < request.exec_time else
                            request.exec_time)
            else:
                run_time = request.exec_time

            core_to_run = self.core_group.one_idle_core_become_active()
            logging.debug("Shinjuku: Run Request {} on core {} for {} at {}."
                          " Total remaining time: {}"
                          .format(request.idx, core_to_run.core_id,
                                  run_time, self.env.now, request.exec_time))
            core_to_run.set_request(request)

            # Update the remaining request execution time
            request.exec_time -= run_time
            self.env.process(core_to_run.run_request(run_time))

        logging.debug("Shinjuku: Becomes idle at {}" .format(self.env.now))
        self.active = False

    def notified(self, core):
        logging.debug("Shinjuku: Notified at {} by Core {}"
                      .format(self.env.now, core.core_id))
        # Put it at the end of queue for now
        done_request = core.remove_request()
        self.core_group.core_become_idle(core, done_request)
        if done_request.exec_time != 0:
            self.queue.renqueue(done_request)
            logging.debug("Shinjuku: Request {} re-added to queue at {}"
                          .format(done_request.idx, self.env.now))
        else:
            logging.debug("Shinjuku: Request {} done at {}"
                          .format(done_request.idx, self.env.now))
            flow_id = done_request.flow_id
            latency = self.env.now - done_request.start_time
            self.histograms.record_value(flow_id, latency)

        if not self.active:
            self.env.process(self.become_active())


class WorkerCore(object):

    notification_receiver = None
    executing_request = None

    def __init__(self, env, histograms, core_id):
        self.env = env
        self.histograms = histograms
        self.core_id = core_id

    def set_request(self, request):
        if(self.executing_request):
            raise "WorkerCore: request already set"
        self.executing_request = request

    def set_notifier(self, notifier):
        self.notification_receiver = notifier

    def run_request(self, time):
        yield self.env.timeout(time)
        self.notify()

    def remove_request(self):
        r = self.executing_request
        self.executing_request = None
        return r

    def notify(self):
        self.notification_receiver.notified(self)


class CoreScheduler(object):
    def __init__(self, env, histograms, core_id, flow_config):
        self.env = env
        self.histograms = histograms
        self.core_id = core_id
        self.flow_config = flow_config
        self.active = False

    def set_queue(self, queue):
        self.queue = queue

    def set_host(self, host):
        self.host = host

    def active(self):
        return self.active

    def process_request(self, request):
        logging.debug('Scheduler: Assigning request {} to core {} at {}'
                      .format(request.idx, self.core_id, self.env.now))

        time_slice = self.flow_config[request.flow_id].get('time_slice')
        if (time_slice == 0 or time_slice >= request.exec_time):
            yield self.env.timeout(request.exec_time)
            latency = self.env.now - request.start_time
            logging.debug('Scheduler: Request {} Latency {}'.format
                          (request.idx, latency))
            flow_id = request.flow_id
            self.histograms.record_value(flow_id, latency)
            logging.debug('Scheduler: Request {} finished execution at core {}'
                          ' at {}'.format(request.idx, self.core_id,
                                          self.env.now))
        else:
            yield self.env.timeout(time_slice +
                                   self.flow_config[request.flow_id].
                                   get('preemption'))
            request.exec_time -= time_slice
            request.expected_length -= time_slice
            logging.debug('Scheduler: Request {} preempted at core {} at {}'
                          .format(request.idx, self.core_id, self.env.now))

            # FIXME Add enqueue cost/lock
            # Add the unfinished request to the queue
            # print request
            # print request.idx
            # print self.queue.q
            self.queue.renqueue(request)
            # print self.queue.q

    # Start up if not already looping
    def become_active(self):
        if (self.active):
            return
        request = None

        # Become idle only after process finishes
        self.active = True
        logging.debug("CoreScheduler: Core {} becomes active at {}"
                      .format(self.core_id, self.env.now))
        while not self.queue.empty():
            # Keep waiting for request
            req = self.queue.resource.request()

            # Wait for my turn of the lock
            logging.debug("CoreScheduler: Core {} acquiring lock"
                          .format(self.core_id, self.env.now))
            yield req
            logging.debug("CoreScheduler: Core {} got lock at {}"
                          .format(self.core_id, self.env.now))

            request = self.queue.dequeue()

            total_time = self.env.now - request.start_time + request.exec_time
            target_slo = self.flow_config[request.flow_id].get('slo',
                                                               float('inf'))
            if ((total_time > target_slo and
                 self.flow_config[request.flow_id].get('drop'))):
                self.histograms.drop_request(request.flow_id)
                self.env.timeout(0.0)
                self.queue.resource.release(req)
            else:
                p = None
                if request is not None:
                    p = self.env.process(self.process_request(request))
                    # Take into account the dequeuing cost
                    yield self.env.timeout(self.queue.dequeue_time)

                    self.queue.resource.release(req)

                if p:
                    yield p

        logging.debug("CoreScheduler: Core {} becomes idle at {}"
                      .format(self.core_id, self.env.now))
        self.active = False

        if self.host:
            self.host.core_become_idle(self, request)
