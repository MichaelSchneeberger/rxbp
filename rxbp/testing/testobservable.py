from typing import List, Iterable

from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ValueType


class TestObservable(Observable):
    def __init__(self, observer: Observer = None):
        self.observer = observer

        self.is_disposed = False

    def on_next_single(self, val: ValueType):
        return self.on_next([val])

    def on_next_list(self, val: List[ValueType]):
        return self.on_next(val)

    def on_next_iter(self, val: Iterable[ValueType]):
        def gen():
            yield from val

        return self.on_next(gen())

    def on_next(self, val):
        return self.observer.on_next(val)

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer

        def dispose_func():
            self.is_disposed = True

        return Disposable(dispose_func)
