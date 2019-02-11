from typing import Callable, Any

from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer


class ZipWithIndexObservable(Observable):
    def __init__(self, source, selector: Callable[[Any, int], Any]):
        self.source = source
        self.selector = (lambda v, i: (v, i)) if selector is None else selector

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        count = [0]

        def on_next(v):
            def map_gen():
                for e in v():
                    result = self.selector(e, count[0])
                    count[0] += 1
                    yield result

            return observer.on_next(map_gen)

        class ZipCountObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = ZipCountObserver()
        return self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)
