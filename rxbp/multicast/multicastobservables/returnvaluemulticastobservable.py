from dataclasses import dataclass
from typing import Any

from rx.disposable import Disposable

from rxbp.mixins.schedulermixin import SchedulerMixin
from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


@dataclass
class ReturnValueMultiCastObservable(MultiCastObservableMixin):
    val: Any
    scheduler: SchedulerMixin

    def observe(self, info: MultiCastObserverInfo) -> Disposable:
        def scheduler_action(_, __):
            info.observer.on_next(self.val)
            info.observer.on_completed()

        return self.scheduler.schedule(scheduler_action)
