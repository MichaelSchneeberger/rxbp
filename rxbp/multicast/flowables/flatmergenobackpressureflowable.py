from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.multicast.observables.flatmapnobackpressureobservable import FlatMapNoBackpressureObservable
from rxbp.multicast.observables.flatmergenobackpressureobservable import FlatMergeNoBackpressureObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class FlatMergeNoBackpressureFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
    ):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(flowable: FlowableBase):
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatMergeNoBackpressureObservable(
            source=subscription.observable,
            selector=observable_selector,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        # base becomes undefined after flat mapping
        base = None

        return Subscription(SubscriptionInfo(base=base), observable=observable)