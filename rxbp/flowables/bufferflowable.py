from rxbp.flowablebase import FlowableBase
from rxbp.observables.bufferobservable import BufferObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class BufferFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, buffer_size: int = None):
        super().__init__()

        self._source = source
        self._buffer_size = buffer_size

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        observable = BufferObservable(
            subscription.observable,
            buffer_size=self._buffer_size,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        return Subscription(info=subscription.info, observable=observable)
