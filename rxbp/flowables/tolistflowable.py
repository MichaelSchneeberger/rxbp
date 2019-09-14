from rxbp.flowablebase import FlowableBase
from rxbp.observables.tolistobservable import ToListObservable
from rxbp.selectors.bases import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class ToListFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ToListObservable(source=subscription.observable)

        # to_list emits exactly one element
        base = NumericalBase(1)

        return Subscription(SubscriptionInfo(base=base), observable=observable)