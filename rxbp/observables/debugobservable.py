from dataclasses import dataclass
from traceback import FrameSummary
from typing import Optional, Callable, Any, List

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.observers.debugobserver import DebugObserver
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler


@dataclass
class DebugObservable(Observable):
    source: Observable
    name: Optional[str]
    on_next: Callable[[Any], AckMixin]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_sync_ack: Callable[[AckMixin], None]
    on_async_ack: Callable[[AckMixin], None]
    on_subscribe: Callable[[ObserverInfo], None]
    on_raw_ack: Callable[[AckMixin], None]
    subscribe_scheduler: Scheduler
    stack: List[FrameSummary]

    def observe(self, observer_info: ObserverInfo):
        self.on_subscribe(observer_info)

        observer = DebugObserver(
            source=observer_info.observer,
            name=self.name,
            on_next_func=self.on_next,
            on_completed_func=self.on_completed,
            on_error_func=self.on_error,
            on_subscribe=self.on_subscribe,
            on_sync_ack=self.on_sync_ack,
            on_async_ack=self.on_async_ack,
            on_raw_ack=self.on_raw_ack,
            stack=self.stack,
            has_scheduled_next=False
        )

        def action(_, __):
            observer.has_scheduled_next = True
        self.subscribe_scheduler.schedule(action)

        return self.source.observe(observer_info.copy(
            observer=observer,
        ))
