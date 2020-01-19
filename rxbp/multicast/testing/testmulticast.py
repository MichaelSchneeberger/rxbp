import rx

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.testing.testrxobservable import TestRxObservable
from rxbp.multicast.typing import MultiCastValue


class TestMultiCast(MultiCastBase):
    def __init__(self):
        self.observable = TestRxObservable()

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.observable

    def on_next(self, v):
        self.observable.observer.on_next(v)

    def on_completed(self):
        self.observable.observer.on_completed()

    def on_error(self, exc):
        self.observable.observer.on_error(exc)
