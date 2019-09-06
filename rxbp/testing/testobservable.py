from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo


class TestObservable(Observable):
    def __init__(self, observer: Observer = None):
        self.observer = observer

    def on_next_single(self, val):
        def gen():
            yield val

        return self.on_next(gen)

    def on_next_seq(self, val):
        def gen():
            yield from val

        return self.on_next(gen)

    def on_next(self, val):
        return self.observer.on_next(val)

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer
        return Disposable()
