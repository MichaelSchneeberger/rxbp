from typing import Callable, Iterator

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.maptoiteratorobservable import MapToIteratorObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class MapToIteratorFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        super().__init__()

        self._source = source
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = MapToIteratorObservable(source=subscription.observable, func=self._func)
        return subscription.copy(observable=observable)