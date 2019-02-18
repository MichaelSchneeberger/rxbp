from typing import Callable, Any

from rxbp.ack import Continue
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool]):
        self.source = source
        self.predicate = predicate

    def unsafe_subscribe(self, observer: Observer, scheduler: Scheduler,
                         subscribe_scheduler: Scheduler):
        def on_next(v):
            def gen_filtered_iterable():
                for e in v():
                    if self.predicate(e):
                        yield e

            should_run = list(gen_filtered_iterable())
            if should_run:
                def gen_output():
                    yield from should_run
                return observer.on_next(gen_output)
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
        return self.source.unsafe_subscribe(filter_observer, scheduler, subscribe_scheduler)
