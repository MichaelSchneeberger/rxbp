from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class BufferFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, buffer_size: int):
        super().__init__()

        self._source = source
        self._buffer_size = buffer_size

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        source = self

        class BufferObservable(Observable):
            def observe(self, observer_info: ObserverInfo):
                observer = observer_info.observer

                buffered_observer = BackpressureBufferedObserver(
                    underlying=observer, scheduler=subscriber.scheduler,
                    subscribe_scheduler=subscriber.subscribe_scheduler,
                    buffer_size=source._buffer_size,
                )
                disposable = subscription.observable.observe(ObserverInfo(observer=buffered_observer))
                return disposable
        observable = BufferObservable()

        return Subscription(info=subscription.info, observable=observable)
