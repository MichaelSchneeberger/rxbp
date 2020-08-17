from dataclasses import dataclass
from typing import Callable, Any

from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.filterobservable import FilterObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FilterFlowable(FlowableBaseMixin):
    source: FlowableBaseMixin
    predicate: Callable[[Any], bool]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber)

        observable = FilterObservable(
            source=subscription.observable,
            predicate=self.predicate,
        )

        return subscription.copy(observable=observable)
