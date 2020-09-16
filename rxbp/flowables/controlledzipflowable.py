from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class ControlledZipFlowable(FlowableMixin):
    left: FlowableMixin
    right: FlowableMixin
    stack: List[FrameSummary]
    request_left: Callable[[Any, Any], bool]
    request_right: Callable[[Any, Any], bool]
    match_func: Callable[[Any, Any], bool]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self.left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self.right.unsafe_subscribe(subscriber=subscriber)

        observable = ControlledZipObservable(
            left=left_subscription.observable,
            right=right_subscription.observable,
            request_left=self.request_left,
            request_right=self.request_right,
            match_func=self.match_func,
            scheduler=subscriber.scheduler,
            stack=self.stack,
        )

        return init_subscription(observable=observable)
