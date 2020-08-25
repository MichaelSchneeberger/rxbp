from dataclasses import dataclass
from typing import Any, Callable

import rx

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


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
                    self.source.on_next([self.lazy_val()])
                self.source.on_completed()

        return self.source.observe(observer_info.copy(
            observer=DefaultIfEmptyMultiCastObserver(
                source=observer_info.observer,
                lazy_val=self.lazy_val,
                found=False,
            )
        ))