from rx import config
from rx.concurrency import current_thread_scheduler

from rxbackpressure import BlockingFuture
from rxbackpressure.core.backpressurebase import BackpressureBase


class ControlledBackpressure(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure

        self._lock = config["concurrency"].RLock()
        self.scheduler = current_thread_scheduler
        self.requests = []

    def request(self, number_of_items):
        future = BlockingFuture()

        def action(a, s):
            is_first = False
            with self._lock:
                if len(self.requests) == 0:
                    is_first = True
                self.requests.append((future, number_of_items, 0))
            if is_first:
                self.backpressure.request(number_of_items)

        self.scheduler.schedule(action)
        return future

    def update(self):
        def action(a, s):
            future, number_of_items, current_number = self.requests[0]
            new_request = (future, number_of_items, current_number + 1)
            if new_request[2] == number_of_items:
                # print('set future')
                future.set(number_of_items)
                has_requests = False
                with self._lock:
                    self.requests.pop()
                    if len(self.requests) > 0:
                        has_requests = True
                if has_requests:
                    future, number_of_items, current_number = self.requests[0]
                    self.backpressure.request(number_of_items)
            else:
                with self._lock:
                    self.requests[0] = new_request

        self.scheduler.schedule(action)