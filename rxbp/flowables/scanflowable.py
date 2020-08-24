from typing import Any, Callable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.scanobservable import ScanObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ScanFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        super().__init__()

        self._source = source
        self._func = func
        self._initial = initial

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ScanObservable(source=subscription.observable, func=self._func, initial=self._initial)
        return init_subscription(observable=observable)