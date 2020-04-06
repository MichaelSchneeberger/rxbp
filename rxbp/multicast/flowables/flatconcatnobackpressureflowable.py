from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.multicast.observables.flatconcatnobackpressureobservable import FlatConcatNoBackpressureObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlatConcatNoBackpressureFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            selector: Callable[[Any], FlowableBase],
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self._source = source
        self._selector = selector
        self._subscribe_scheduler = subscribe_scheduler

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self._selector(elem)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatConcatNoBackpressureObservable(
            source=subscription.observable,
            selector=observable_selector,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=self._subscribe_scheduler,
        )

        # base becomes undefined after flat mapping
        base = None

        return Subscription(BaseAndSelectors(base=base), observable=observable)