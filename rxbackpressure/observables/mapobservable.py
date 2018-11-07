from typing import Callable, Any

from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer


class MapObservable(Observable):
    def __init__(self, source, selector: Callable[[Any], Any]):
        self.source = source
        self.func = selector

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        def on_next(v):
            result = self.func(v)
            return observer.on_next(result)

        class MapObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = MapObserver()
        self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)