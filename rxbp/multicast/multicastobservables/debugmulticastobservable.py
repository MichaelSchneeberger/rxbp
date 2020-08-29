from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.debugmulticastobserver import DebugMultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.scheduler import Scheduler


@dataclass
class DebugMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    multicast_scheduler: Scheduler
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_observe: Callable[[MultiCastObserverInfo], None]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        self.on_observe(observer_info)

        observer = DebugMultiCastObserver(
            source=observer_info.observer,
            on_next_func=self.on_next,
            on_completed_func=self.on_completed,
            on_error_func=self.on_error,
        )

        def action(_, __):
            observer.has_scheduled_next = True
        self.multicast_scheduler.schedule(action)

        return self.source.observe(observer_info.copy(
            observer=observer,
        ))
