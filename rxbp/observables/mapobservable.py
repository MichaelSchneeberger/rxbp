from typing import Callable, Any, List

from rx.concurrency.schedulerbase import SchedulerBase

from rxbp.observable import Observable
from rxbp.observer import Observer


class MapObservable(Observable):
    def __init__(self, source, selector: Callable[[Any], Any]):
        self.source = source
        self.selector = selector

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        def on_next(v):
            def map_gen():
                for e in v():
                    yield self.selector(e)

            return observer.on_next(map_gen)

        class MapObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = MapObserver()
        return self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)
