from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observesubscription import ObserveSubscription
from rxbp.subscriber import Subscriber


class BufferFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, buffer_size: int):
        super().__init__(base=source.base)

        self._source = source
        self._buffer_size = buffer_size

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_obs, selector = self._source.unsafe_subscribe(subscriber=subscriber)

        source = self

        class BufferObservable(Observable):
            def observe(self, subscription: ObserveSubscription):
                observer = subscription.observer

                buffered_subscriber = BackpressureBufferedObserver(
                    underlying=observer, scheduler=subscriber.scheduler,
                    subscribe_scheduler=subscriber.subscribe_scheduler,
                    buffer_size=source._buffer_size,
                )
                disposable = source_obs.observe(buffered_subscriber)
                return disposable
        obs = BufferObservable()

        return obs, selector
