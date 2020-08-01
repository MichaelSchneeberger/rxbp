from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class FilterObserver(Observer):
    observer: Observer
    predicate: Callable[[Any], bool]

    def on_next(self, elem: ElementType):
        def gen_filtered_iterable():
            for e in elem:
                if self.predicate(e):
                    yield e

        return self.observer.on_next(gen_filtered_iterable())

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()
