import rx

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.testing.testmulticastobservable import TestMultiCastObservable
from rxbp.multicast.typing import MultiCastItem


class TestMultiCast(MultiCastMixin):
    def __init__(self):
        self.observable = TestMultiCastObservable()

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:
        return self.observable

    def on_next(self, v):
        self.observable.observer.on_next(v)

    def on_completed(self):
        self.observable.observer.on_completed()

    def on_error(self, exc):
        self.observable.observer.on_error(exc)
