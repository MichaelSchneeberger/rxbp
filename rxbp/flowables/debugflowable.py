from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.acknowledgement.ack import Ack
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.debugobservable import DebugObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber


@dataclass
class DebugFlowable(FlowableMixin):
    source: FlowableMixin
    name: str
    on_next: Callable[[Any], Ack]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_sync_ack: Callable[[Ack], None]
    on_async_ack: Callable[[Ack], None]
    on_subscribe: Callable[[Subscriber], None]
    on_observe: Callable[[ObserverInfo], None]
    on_raw_ack: Callable[[Ack], None]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber):
        self.on_subscribe(subscriber)
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = DebugObservable(
            source=subscription.observable,
            name=self.name,
            on_next=self.on_next,
            on_completed=self.on_completed,
            on_error=self.on_error,
            on_observe=self.on_observe,
            on_sync_ack=self.on_sync_ack,
            on_async_ack=self.on_async_ack,
            on_raw_ack=self.on_raw_ack,
            subscriber=subscriber,
            stack=self.stack,
        )

        return subscription.copy(observable=observable)
