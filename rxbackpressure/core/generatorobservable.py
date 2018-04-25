from rx import config
from rx.concurrency import current_thread_scheduler
from rx.subjects import Subject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
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
        if isinstance(number_of_items, StopRequest):
            self.requests = []
            return

        subject = Subject()
        self.requests.append((subject, number_of_items))
        consume_requests = False
        with self.lock:
            if self.future_value:
                consume_requests = True
        if consume_requests:
            self.consume_requests()
        return subject

    def set_value(self, val):
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
    def __init__(self, obs, scheduler=None):
        super().__init__()

        self._obs = obs
        self._scheduler = scheduler

    def _subscribe_core(self, observer, scheduler):
        scheduler = self._scheduler or scheduler

        backpressure = GeneratorBackpressure(observer, scheduler=scheduler)
        observer.subscribe_backpressure(backpressure)
        self._obs.first().subscribe(lambda v: backpressure.set_value(v))

