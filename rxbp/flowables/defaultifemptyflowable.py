from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.defaultifemptyobservable import DefaultIfEmptyObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class DefaultIfEmptyFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            lazy_val: Callable[[], Any],
    ):
        super().__init__()

        self._source = source
        self.lazy_val = lazy_val

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=DefaultIfEmptyObservable(
                source=subscription.observable,
                lazy_val=self.lazy_val,
            ),
        )
