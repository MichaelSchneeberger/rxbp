from typing import Callable, Any

from rxbp.observer import Observer
from rxbp.typing import ElementType


class ScanObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        self.observer = observer
        self.func = func
        self.acc = initial

    def on_next(self, elem: ElementType):
        def scan_gen():
            for v in elem:
                val = self.func(self.acc, v)
                self.acc = val
                yield val

        ack = self.observer.on_next(scan_gen())
        return ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()
