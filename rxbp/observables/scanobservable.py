import itertools
from typing import Callable, Any

from rxbp.ack import continue_ack, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer


class ScanObservable(Observable):
    def __init__(self, source: Observable, func: Callable[[Any, Any], Any], initial: Any):
        self.source = source
        self.func = func
        self.acc = initial

    def observe(self, observer: Observer):

        def on_next(v):

            def scan_gen():
                for elem in v():
                    val = self.func(self.acc, elem)
                    self.acc = val
                    yield val

            ack = observer.on_next(scan_gen)
            return ack

        class ScanObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        scan_observer = ScanObserver()
        return self.source.observe(scan_observer)
