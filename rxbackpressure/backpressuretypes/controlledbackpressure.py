from rx import config
from rx.concurrency import current_thread_scheduler, immediate_scheduler
from rx.subjects import Subject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.backpressurebase import BackpressureBase


class ControlledBackpressure(BackpressureBase):
    def __init__(self, backpressure, scheduler):
        self.backpressure = backpressure

        self._lock = config["concurrency"].RLock()
        self.scheduler = scheduler or current_thread_scheduler
        self.requests = []
        self.is_completed = False

    def request(self, number_of_items):
        # print('request opening {}'.format(number_of_items))

        if isinstance(number_of_items, StopRequest):
            # print('stop request')
            self.is_completed = True
            self.backpressure.request(number_of_items)
            return

        future = Subject()

        # def action(a, s):
        is_first = False
        with self._lock:
            if len(self.requests) == 0:
                is_first = True
            self.requests.append((future, number_of_items, 0))

        # print('is_first = {}'.format(is_first))
        # print(self.requests)
        if is_first:
            self.backpressure.request(number_of_items)

        # self.scheduler.schedule(action)
        return future

    def update(self):
        def action(a, s):
            # print('run update')
            subject, number_of_items, current_number = self.requests[0]
            updated_request = (subject, number_of_items, current_number + 1)

            # is request fulfilled?
            if current_number + 1 == number_of_items:

                subject.on_next(number_of_items)
                subject.on_completed()

                has_next_request = False
                with self._lock:
                    self.requests.pop(0)
                    if len(self.requests) > 0:
                        has_next_request = True

                if has_next_request:
                    subject, number_of_items, current_number = self.requests[0]
                    # print('request {}'.format(number_of_items))
                    self.backpressure.request(number_of_items)
            else:
                with self._lock:
                    self.requests[0] = updated_request

        self.scheduler.schedule(action)
        # immediate_scheduler.schedule(action)