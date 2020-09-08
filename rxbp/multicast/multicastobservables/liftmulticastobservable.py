from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.scheduler import Scheduler


@dataclass
class LiftMultiCastObservable(MultiCastObservable):
    source: MultiCastMixin
    # multicast_scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        def action(_, __):
            try:
                observer_info.observer.on_next([self.source])
                observer_info.observer.on_completed()

            except Exception as exc:
                observer_info.observer.on_error(exc)

        return self.multicast_scheduler.schedule(action)