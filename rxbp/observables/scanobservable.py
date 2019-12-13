from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class ScanObservable(Observable):
    def __init__(self, source: Observable, func: Callable[[Any, Any], Any], initial: Any):
        self.source = source
        self.func = func
        self.initial = initial

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        func = self.func
        initial = self.initial

        class ScanObserver(Observer):
            def __init__(self):
                self.acc = initial

            def on_next(self, elem: ElementType):
                def scan_gen():
                    for v in elem:
                        val = func(self.acc, v)
                        self.acc = val
                        yield val

                # materialized_values = list(scan_gen())
                # def gen():
                #     yield from materialized_values

                ack = observer.on_next(scan_gen())
                return ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        scan_observer = observer_info.copy(ScanObserver())
        return self.source.observe(scan_observer)
