from typing import Callable, Any

from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressure.ack import Continue
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer


class PairwiseObservable(Observable):
    def __init__(self, source, selector: Callable[[Any, Any], Any] = None):
        self.source = source
        self.selector = selector or (lambda v1, v2: (v1, v2))

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):

        is_first = [True]
        last_elem = [None]

        def on_next(v):
            if is_first[0]:
                is_first[0] = False
                last_elem[0] = v
                return Continue()
            else:
                new = self.selector(last_elem[0], v)
                last_elem[0] = v
                ack = observer.on_next(new)
                return ack

        class PairwiseObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = PairwiseObserver()
        self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)