from typing import Callable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.firstobservable import FirstObservable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FirstFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            raise_exception: Callable[[Callable[[], None]], None],
    ):
        super().__init__()

        self._source = source
        self.raise_exception = raise_exception

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = FirstObservable(source=subscription.observable, raise_exception=self.raise_exception)

        return init_subscription(observable=observable)