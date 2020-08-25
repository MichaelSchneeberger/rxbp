from dataclasses import dataclass
from typing import Any, Callable

import rx
from rx import Observable
from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


class DefaultIfEmptyMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            lazy_val: Callable[[], Any],
    ):
        self.source = source
        self.lazy_val = lazy_val

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:

        @dataclass
        class DefaultIfEmptyMultiCastObservable(MultiCastObservable):
            source: MultiCastObservable
            lazy_val: Callable[[], Any]

            def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:

                @dataclass
                class DefaultIfEmptyMultiCastObserver(MultiCastObserver):
                    source: MultiCastObserver
                    lazy_val: Callable[[], Any]
                    found: bool

                    def on_next(self, elem: MultiCastItem) -> None:
                        self.found = True
                        self.source.on_next(elem)

                    def on_error(self, exc: Exception) -> None:
                        self.source.on_error(exc)

                    def on_completed(self) -> None:
                        if not self.found:
                            self.source.on_next(self.lazy_val())
                        self.source.on_completed()

                self.source.observe(observer_info.copy(
                    observer=DefaultIfEmptyMultiCastObserver(
                        source=observer_info.observer,
                        lazy_val=self.lazy_val,
                        found=False,
                    )
                ))

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=DefaultIfEmptyMultiCastObservable(
                source=subscription.observable,
                lazy_val=self.lazy_val,
            ),
        )
