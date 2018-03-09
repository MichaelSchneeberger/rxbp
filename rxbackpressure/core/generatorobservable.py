from rx import config
from rx.concurrency import current_thread_scheduler
from rx.subjects import Subject

from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


class GeneratorBackpressure(BackpressureBase):
    def __init__(self, observer, scheduler=None):
        self.requests = []
        self.observer = observer
        self.future_value = None
        self.lock = config["concurrency"].RLock()
        self.scheduler = scheduler or current_thread_scheduler

    def request(self, number_of_items):
        future = Subject()
        self.requests.append((future, number_of_items))
        consume_requests = False
        with self.lock:
            if self.future_value:
                consume_requests = True
        if consume_requests:
            self.consume_requests()
        return future

    def set_future_value(self, val):
        with self.lock:
            self.future_value = val
        self.consume_requests()

    def consume_requests(self):
        def action(s, a):
            request = None
            with self.lock:
                if len(self.requests) > 0:
                    request = self.requests.pop(0)
            if request:
                future, number_of_items = request
                for _ in range(number_of_items):
                    self.observer.on_next(self.future_value)
                future.on_next(number_of_items)
                future.on_completed()
                self.consume_requests()

        self.scheduler.schedule(action)


class GeneratorObservable(BackpressureObservable):
    def __init__(self, future):
        self.future = future

    def _subscribe_core(self, observer, scheduler):
        backpressure = GeneratorBackpressure(observer, scheduler=scheduler)
        observer.subscribe_backpressure(backpressure, scheduler)
        self.future.subscribe(lambda v: backpressure.set_future_value(v))
