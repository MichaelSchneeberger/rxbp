from rx import config
from rx.concurrency import current_thread_scheduler

from rxbackpressure import BlockingFuture
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.backpressurebase import BackpressureBase


class WindowBackpressure(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure

        self._lock = config["concurrency"].RLock()
        self.scheduler = current_thread_scheduler
        self.requests = []
        self.current_request = None
        self.num_elements_removed = 0
        self.num_elements_req = 0

    def request(self, number_of_items):
        future = BlockingFuture()
        self.requests.append((future, number_of_items, 0))
        self.update()
        return future

    def update(self):
        def action(a, s):
            open_new_request = False
            with self._lock:
                if self.current_request is None and len(self.requests) > 0:
                    open_new_request = True
                    self.current_request = self.requests.pop()
            if open_new_request:
                future, number_of_items, current = self.current_request
                if isinstance(number_of_items, StopRequest):
                    self.backpressure.request(number_of_items)
                    self.requests = []
                else:
                    delta = self.num_elements_req - number_of_items
                    if delta < 0:
                        self.num_elements_req = 0
                        self.backpressure.request(-delta)
                    else:
                        self.num_elements_req = delta
                        self.update()

        self.scheduler.schedule(action)

    def update_current_request(self):
        future, num_of_items, current_num = self.current_request
        if current_num + self.num_elements_removed == num_of_items:
            if self.num_elements_removed > 0:
                self.backpressure.request(self.num_elements_removed)
                self.num_elements_removed = 0
            else:
                future.set(num_of_items)
                self.current_request = None
                self.update()

    def remove_element(self, num=1):
        self.num_elements_removed += num
        self.update_current_request()

    def next_element(self, num=1):
        if self.current_request:
            future, num_of_items, current_num = self.current_request
            current_num += num
            self.current_request = (future, num_of_items, current_num)
            self.update_current_request()

    def finish_current_request(self):
        if self.current_request is not None:
            with self._lock:
                self.requests = []
                future, num_of_items, current_num = self.current_request
                future.set(current_num)
                self.num_elements_req = num_of_items - current_num
                self.current_request = None