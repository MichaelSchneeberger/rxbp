from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.fastfilterobservable import FastFilterObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FastFilterFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            predicate: Callable[[Any], bool],
    ):
        super().__init__()

        self._source = source
        self._predicate = predicate

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber)

        observable = FastFilterObservable(
            source=subscription.observable,
            predicate=self._predicate,
        )

        return Subscription(info=BaseAndSelectors(base=None), observable=observable)