from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.optfilterobservable import OptFilterObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class OptFilterFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            predicate: Callable[[Any], bool],
    ):
        super().__init__()

        self._source = source
        self._predicate = predicate

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber)

        observable = OptFilterObservable(
            source=subscription.observable,
            predicate=self._predicate,
        )

        return Subscription(info=SubscriptionInfo(base=None), observable=observable)