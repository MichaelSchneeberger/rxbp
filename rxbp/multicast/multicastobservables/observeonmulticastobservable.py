from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.mapmulticastobserver import MapMultiCastObserver
from rxbp.multicast.multicastobservers.observeonmulticastobserver import ObserveOnMultiCastObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler


@dataclass
class ObserveOnMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    scheduler: Scheduler
    source_scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        return self.source.observe(observer_info.copy(
            observer=ObserveOnMultiCastObserver(
                next_observer=observer_info.observer,
                scheduler=self.scheduler,
                source_scheduler=self.source_scheduler,
            )
        ))
