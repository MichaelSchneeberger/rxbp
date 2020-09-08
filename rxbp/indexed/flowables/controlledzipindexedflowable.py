from typing import Callable, Any

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ControlledZipIndexedFlowable(FlowableMixin):
    def __init__(
            self,
            left: FlowableMixin,
            right: FlowableMixin,
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ):

        super().__init__()

        self._left = left
        self._right = right
        self._request_left = request_left if request_left is not None else lambda _, __: True
        self._request_right = request_right if request_right is not None else lambda _, __: True
        self._match_func = match_func if match_func is not None else lambda _, __: True

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        return left_subscription.copy(
            observable=ControlledZipObservable(
                left=left_subscription.observable,
                right=right_subscription.observable,
                request_left=self._request_left,
                request_right=self._request_right,
                match_func=self._match_func,
                scheduler=subscriber.scheduler,
            ),
        )