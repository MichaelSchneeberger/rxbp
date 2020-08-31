from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.reduceobservable import ReduceObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ReduceFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        super().__init__()

        self._source = source
        self.func = func
        self.initial = initial

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=ReduceObservable(
                source=subscription.observable,
                func=self.func,
                initial=self.initial,
            ),
        )