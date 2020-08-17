from dataclasses import dataclass

from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.bufferobservable import BufferObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class BufferFlowable(FlowableBaseMixin):
    source: FlowableBaseMixin
    buffer_size: int

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = BufferObservable(
            subscription.observable,
            buffer_size=self.buffer_size,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        return subscription.copy(
            observable=observable,
        )
