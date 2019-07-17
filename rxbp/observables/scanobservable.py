import itertools
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer


class ScanObservable(Observable):
    def __init__(self, source: Observable, func: Callable[[Any, Any], Any], initial: Any):
        self.source = source
        self.func = func
        self.acc = initial

    def observe(self, observer: Observer):

        def on_next(v):
            def scan_func(elem):
                val = self.func(self.acc, elem)
                self.acc = val
                return val

            materialize_data = [scan_func(elem) for elem in v()]

            def scan_gen():
                # for elem in v():
                #     val = self.func(self.acc, elem)
                #     self.acc = val
                #     yield val
                yield from materialize_data

            # materialized_values = list(scan_gen())
            # def gen():
            #     yield from materialized_values

            ack = observer.on_next(scan_gen)
            return ack

        class ScanObserver(Observer):
            @property
            def is_volatile(self):
                return observer.is_volatile

            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        scan_observer = ScanObserver()
        return self.source.observe(scan_observer)
