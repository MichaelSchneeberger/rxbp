from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.multicast.observables.flatmergenobackpressureobservable import FlatMergeNoBackpressureObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlatMergeNoBackpressureFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            selector: Callable[[Any], FlowableBase],
    ):
        super().__init__()

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(val: Any):
            flowable = self._selector(val)
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

        return Subscription(BaseAndSelectors(base=base), observable=observable)