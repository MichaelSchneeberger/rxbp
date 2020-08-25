from dataclasses import dataclass
from traceback import FrameSummary
from typing import Optional, Callable, Any, List

from rxbp.acknowledgement.ack import Ack
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.debugobserver import DebugObserver
from rxbp.subscriber import Subscriber


@dataclass
class DebugObservable(Observable):
    source: Observable
    name: Optional[str]
    on_next: Callable[[Any], Ack]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_sync_ack: Callable[[Ack], None]
    on_async_ack: Callable[[Ack], None]
    on_subscribe: Callable[[ObserverInfo, Subscriber], None]
    on_raw_ack: Callable[[Ack], None]
    subscriber: Subscriber
    stack: List[FrameSummary]

    def observe(self, observer_info: ObserverInfo):
        self.on_subscribe(observer_info, self.subscriber)

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
        self.subscriber.subscribe_scheduler.schedule(action)

        return self.source.observe(observer_info.copy(
            observer=observer,
        ))
