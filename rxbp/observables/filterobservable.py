from typing import Callable, Any

from rx.concurrency.schedulerbase import SchedulerBase

from rxbp.ack import Continue
from rxbp.observable import Observable
from rxbp.observer import Observer


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool]):
        self.source = source
        self.predicate = predicate

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        def on_next(v):
            should_run = self.predicate(v)
            if should_run:
                return observer.on_next(v)
            else:
                return Continue()

        class FilterObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        filter_observer = FilterObserver()
        self.source.unsafe_subscribe(filter_observer, scheduler, subscribe_scheduler)
