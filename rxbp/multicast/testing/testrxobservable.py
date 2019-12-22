from typing import Optional

from rx import Observable
from rx.core import typing
from rx.disposable import Disposable


class TestRxObservable(Observable):
    def __init__(self):
        super().__init__()

        self.observer = None

        self.is_disposed = False

    def _subscribe_core(self,
                        observer: typing.Observer,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        self.observer = observer

        def dispose_func():
            self.is_disposed = True

        return Disposable(dispose_func)

    def on_next(self, val):
        self.observer.on_next(val)

    def on_completed(self):
        self.observer.on_completed()

    def on_error(self, exc):
        self.observer.on_error(exc)


