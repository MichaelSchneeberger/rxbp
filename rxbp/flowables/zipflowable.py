from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.zipobservable import ZipObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class ZipFlowable(FlowableMixin):
    left: FlowableMixin
    right: FlowableMixin
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self.left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self.right.unsafe_subscribe(subscriber=subscriber)

        return left_subscription.copy(
            observable=ZipObservable(
                left=left_subscription.observable,
                right=right_subscription.observable,
                stack=self.stack,
            ),
        )
