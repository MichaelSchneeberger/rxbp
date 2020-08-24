from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.firstobservable import FirstObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FirstFlowable(FlowableMixin):
    source: FlowableMixin
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=FirstObservable(
                source=subscription.observable,
                stack=self.stack,
            ),
        )
