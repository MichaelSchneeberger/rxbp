from dataclasses import dataclass
from typing import Any

from rx.disposable import Disposable

from rxbp.mixins.schedulermixin import SchedulerMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


@dataclass
class ReturnValueMultiCastObservable(MultiCastObservable):
    value: Any
    subscribe_scheduler: SchedulerMixin

    def observe(self, info: MultiCastObserverInfo) -> Disposable:
        def scheduler_action(_, __):
            info.observer.on_next([self.value])
            info.observer.on_completed()

        return self.subscribe_scheduler.schedule(scheduler_action)
