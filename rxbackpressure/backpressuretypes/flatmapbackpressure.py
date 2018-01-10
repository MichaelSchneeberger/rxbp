from rx import config
from rx.concurrency import immediate_scheduler

from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.internal.blockingfuture import BlockingFuture


class FlatMapBackpressure(BackpressureBase):
    def __init__(self, scheduler=None):
        self.scheduler = scheduler or immediate_scheduler
        self.source_backpressure_list = []
        self.buffer = []
        self.requests = []
        self.is_running = False
        self._lock = config["concurrency"].RLock()
        self.is_completed = False

    def add_backpressure(self, backpressure):
        # print('add backpressure')
        self.source_backpressure_list.append(backpressure)
        self._request_source()

    def on_completed(self):
        self.is_completed = True
        self._request_source()

    def request(self, number_of_items) -> BlockingFuture:
        # print('request flatmap {}'.format(number_of_items))
        future = BlockingFuture()
        self.requests.append((future, number_of_items))
        self._request_source()
        return future

    def _request_source(self):
        start_running = False
        complete_requests = False
        with self._lock:
            if not self.is_running and len(self.requests):
                if len(self.source_backpressure_list):
                    start_running = True
                    self.is_running = True
                elif self.is_completed:
                    complete_requests = True
                    self.is_running = True

        if start_running:
            def scheduled_action(a, s):
                future, number_of_items = self.requests[0]

                def handle_request(returned_num_of_items):
                    # print('2 returned_num_of_items = %s' % returned_num_of_items)
                    if returned_num_of_items < number_of_items:
                        # current source completed

                        self.source_backpressure_list.pop(0)
                        d_number_of_items = number_of_items - returned_num_of_items
                        self.requests[0] = future, d_number_of_items
                    else:
                        # current request completed
                        self.requests.pop(0)
                        future.set(returned_num_of_items)
                    self.is_running = False
                    self._request_source()

                # backpressure of first child
                self.source_backpressure_list[0] \
                    .request(number_of_items) \
                    .map(handle_request)

            self.scheduler.schedule(scheduled_action)
        elif complete_requests:
            def scheduled_action(a, s):
                for request in self.requests:
                    future, number_of_items = request
                    future.set(0)

            self.scheduler.schedule(scheduled_action)
