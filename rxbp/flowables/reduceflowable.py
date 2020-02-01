from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.reduceobservable import ReduceObservable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ReduceFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        super().__init__()

        self._source = source
        self.func = func
        self.initial = initial

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ReduceObservable(
            source=subscription.observable,
            func=self.func,
            initial=self.initial,
        )

        # to_list emits exactly one element
        base = NumericalBase(1)

        return Subscription(BaseAndSelectors(base=base), observable=observable)