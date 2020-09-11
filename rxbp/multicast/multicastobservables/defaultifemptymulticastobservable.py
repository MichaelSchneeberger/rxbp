from dataclasses import dataclass
from typing import Any, Callable

import rx

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.defaultifemptymulticastobserver import DefaultIfEmptyMultiCastObserver


@dataclass
class DefaultIfEmptyMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        return self.source.observe(observer_info.copy(
            observer=DefaultIfEmptyMultiCastObserver(
                source=observer_info.observer,
                lazy_val=self.lazy_val,
            )
        ))