from typing import Callable, Any

from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.defaultifemptyobservable import DefaultIfEmptyObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class DefaultIfEmptyFlowable(FlowableBaseMixin):
    def __init__(
            self,
            source: FlowableBaseMixin,
            lazy_val: Callable[[], Any],
    ):
        super().__init__()

        self._source = source
        self.lazy_val = lazy_val

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = DefaultIfEmptyObservable(source=subscription.observable, lazy_val=self.lazy_val)

        base = None

        return init_subscription(BaseAndSelectors(base=base), observable=observable)