from dataclasses import dataclass
from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.firstordefaultobservable import FirstOrDefaultObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FirstOrDefaultFlowable(FlowableMixin):
    source: FlowableMixin
    lazy_val: Callable[[], Any]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=FirstOrDefaultObservable(
                source=subscription.observable,
                lazy_val=self.lazy_val,
            ),
        )