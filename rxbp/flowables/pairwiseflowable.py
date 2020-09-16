from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class PairwiseFlowable(FlowableMixin):
    def __init__(self, source: FlowableMixin):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(observable=PairwiseObservable(
            source=subscription.observable,
        ))
