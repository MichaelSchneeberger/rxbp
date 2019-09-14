from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.scanobservable import ScanObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ScanFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
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
        return Subscription(info=subscription.info, observable=observable)