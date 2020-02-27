from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.mapobservable import MapObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class MapFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, func: Callable[[ValueType], Any]):
        super().__init__()

        self._source = source
        self._selector = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = MapObservable(source=subscription.observable, selector=self._selector)
        return subscription.copy(observable=observable)