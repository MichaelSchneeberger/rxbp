from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber
from rxbp.subscriber import Subscriber


class BufferFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_obs, selector = self._source.unsafe_subscribe(subscriber=subscriber)

        class BufferObservable(Observable):
            def observe(self, observer):
                buffered_subscriber = BufferedSubscriber(
                    observer=observer, scheduler=subscriber.scheduler, buffer_size=1000)
                disposable = source_obs.observe(buffered_subscriber)
                return disposable
        obs = BufferObservable()

        return obs, selector
