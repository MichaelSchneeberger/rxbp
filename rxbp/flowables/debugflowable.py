from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.debugobservable import DebugObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


# todo: assert subscribe_scheduler is active
@dataclass
class DebugFlowable(FlowableBaseMixin):
    source: FlowableBaseMixin
    name: str
    on_next: Callable[[Any], AckMixin]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_sync_ack: Callable[[AckMixin], None]
    on_async_ack: Callable[[AckMixin], None]
    on_subscribe: Callable[[ObserverInfo], None]
    on_raw_ack: Callable[[AckMixin], None]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber):
        print(f'{self.name}.on_subscribe( subscribe_scheduler={subscriber.subscribe_scheduler} )')

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = DebugObservable(
            source=subscription.observable,
            name=self.name,
            on_next=self.on_next,
            on_completed=self.on_completed,
            on_error=self.on_error,
            on_subscribe=self.on_subscribe,
            on_sync_ack=self.on_sync_ack,
            on_async_ack=self.on_async_ack,
            on_raw_ack=self.on_raw_ack,
            subscribe_scheduler=subscriber.subscribe_scheduler,
            stack=self.stack,
        )

        return subscription.copy(observable=observable)
