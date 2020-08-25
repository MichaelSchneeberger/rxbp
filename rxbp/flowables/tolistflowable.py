from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.tolistobservable import ToListObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ToListFlowable(FlowableMixin):
    def __init__(self, source: FlowableMixin):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ToListObservable(source=subscription.observable)

        return subscription.copy(observable=observable)