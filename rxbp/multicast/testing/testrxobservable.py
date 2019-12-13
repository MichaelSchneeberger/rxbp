from typing import Optional

from rx import Observable
from rx.core import typing
from rx.disposable import Disposable


class TestRxObservable(Observable):
    def __init__(self):
        super().__init__()

        self.observer = None

    def _subscribe_core(self,
                        observer: typing.Observer,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        self.observer = observer
        return Disposable()


