from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class FlatMapFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, selector: Callable[[Any], FlowableBase]):
        super().__init__()

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self._selector(elem)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatMapObservable(source=subscription.observable, selector=observable_selector,
                                scheduler=subscriber.scheduler, subscribe_scheduler=subscriber.subscribe_scheduler)

        # base becomes undefined after flat mapping
        base = None

        return Subscription(SubscriptionInfo(base=base), observable=observable)