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
        super().__init__(base=source.base, selectable_bases=source.selectable_bases)

        self._source = source
        self._func = func
        self._initial = initial

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = ScanObservable(source=source_observable, func=self._func, initial=self._initial)
        return obs, source_selectors