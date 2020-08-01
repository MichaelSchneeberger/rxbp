from dataclasses import dataclass
from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.filterobservable import FilterObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FilterFlowable(FlowableMixin):
    source: FlowableMixin
    predicate: Callable[[Any], bool]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber)

        observable = FilterObservable(
            source=subscription.observable,
            predicate=self.predicate,
        )

        return subscription.copy(observable=observable)
