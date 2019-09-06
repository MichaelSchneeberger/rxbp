from rxbp.flowablebase import FlowableBase
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class RepeatFirstFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        # unknown base, depends on the back-pressure
        base = None

        super().__init__(base=base)

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = RepeatFirstObservable(source=source_observable, scheduler=subscriber.scheduler)
        return obs, source_selectors