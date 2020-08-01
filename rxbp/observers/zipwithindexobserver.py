from typing import Callable, Any

from rxbp.observer import Observer
from rxbp.typing import ElementType


class ZipCountObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            selector: Callable[[Any, int], Any],
    ):
        self.observer = observer
        self.selector = selector

        self.count = 0

    def on_next(self, elem: ElementType):
        def map_gen():
            for v in elem:
                result = self.selector(v, self.count)
                self.count += 1
                yield result

        return self.observer.on_next(map_gen())

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()