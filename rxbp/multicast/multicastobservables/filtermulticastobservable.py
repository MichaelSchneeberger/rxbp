from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.filtermulticastobserver import FilterMultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    predicate: Callable[[MultiCastItem], bool]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        return self.source.observe(observer_info.copy(
            observer=FilterMultiCastObserver(
                source=observer_info.observer,
                predicate=self.predicate,
            )
        ))
