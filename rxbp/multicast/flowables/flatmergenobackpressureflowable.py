from dataclasses import dataclass
from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.observables.flatmergenobackpressureobservable import FlatMergeNoBackpressureObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FlatMergeNoBackpressureFlowable(FlowableMixin):
    source: FlowableMixin
    selector: Callable[[Any], FlowableMixin]
    subscribe_scheduler: Scheduler

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        assert self.subscribe_scheduler == subscriber.subscribe_scheduler

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(val: Any):
            flowable = self.selector(val)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        return subscription.copy(observable=FlatMergeNoBackpressureObservable(
            source=subscription.observable,
            selector=observable_selector,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,       # todo: why not taken from subscriber?
        ))