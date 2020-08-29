from dataclasses import dataclass
from typing import Optional

import rx
from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


class TestMultiCastObservable(MultiCastObservable):
    def __init__(self):
        observer: Optional[MultiCastObserver] = None
        self.is_disposed = False

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        self.observer = observer_info.observer

        def dispose_func():
            self.is_disposed = True

        return Disposable(dispose_func)

    def on_next_single(self, val):
        self.observer.on_next([val])

    def on_completed(self):
        self.observer.on_completed()

    def on_error(self, exc):
        self.observer.on_error(exc)


