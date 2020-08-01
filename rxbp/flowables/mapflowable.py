from dataclasses import dataclass
from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.mapobservable import MapObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


@dataclass
class MapFlowable(FlowableMixin):
    source: FlowableMixin
    func: Callable[[ValueType], Any]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        observable = MapObservable(source=subscription.observable, func=self.func)
        return subscription.copy(observable=observable)