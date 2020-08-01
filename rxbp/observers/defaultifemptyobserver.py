import types
from typing import Callable, Any

from rxbp.observer import Observer
from rxbp.typing import ElementType


class DefaultIfEmptyObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            lazy_val: Callable[[], Any],
    ):
        self.observer = observer
        self.lazy_val = lazy_val

        self.is_first = True

    def on_next(self, elem: ElementType):
        self.is_first = False

        self.on_next = types.MethodType(self.observer.on_next, self)  # type: ignore

        return self.observer.on_next(elem)

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            try:
                self.observer.on_next([self.lazy_val()])
                self.observer.on_completed()
            except Exception as exc:
                self.observer.on_error(exc)
        else:
            self.observer.on_completed()
