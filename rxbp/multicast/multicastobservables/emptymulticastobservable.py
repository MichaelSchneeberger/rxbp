from dataclasses import dataclass

import rx

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.scheduler import Scheduler


@dataclass
class EmptyMultiCastObservable(MultiCastObservable):
    source_scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        def action(_, __):
            observer_info.observer.on_completed()

        self.source_scheduler.schedule(action)
