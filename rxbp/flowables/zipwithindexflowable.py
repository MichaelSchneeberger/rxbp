from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class ZipWithIndexFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, selector: Callable[[ValueType], Any]):
        super().__init__()

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ZipWithIndexObservable(source=subscription.observable, selector=self._selector)
        return subscription.copy(observable=observable)