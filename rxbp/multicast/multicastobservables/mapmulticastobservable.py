from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.mapmulticastobserver import MapMultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class MapMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    func: Callable[[MultiCastItem], MultiCastItem]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        return self.source.observe(observer_info.copy(
            observer=MapMultiCastObserver(
                source=observer_info.observer,
                func=self.func,
            )
        ))
