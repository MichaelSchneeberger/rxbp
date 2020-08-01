from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.zipobservable import ZipObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ZipFlowable(FlowableMixin):
    def __init__(
            self,
            left: FlowableMixin,
            right: FlowableMixin,
            func: Callable[[Any, Any], Any] = None,
    ):
        super().__init__()

        self._left = left
        self._right = right
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        return left_subscription.copy(
            observable=ZipObservable(
                left=left_subscription.observable,
                right=right_subscription.observable,
                selector=self._func,
            ),
        )
