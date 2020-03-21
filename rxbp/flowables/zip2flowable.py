from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.baseandselectors import BaseAndSelectors, BaseSelectorsAndSelectorMaps
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class Zip2Flowable(FlowableBase):
    def __init__(
            self,
            left: FlowableBase,
            right: FlowableBase,
            func: Callable[[Any, Any], Any] = None,
    ):
        """
        :param left:
        :param right:
        :param func:
        :param auto_match: if set to False then this Flowable works like a normal join_flowables operation, if set to False then \
        it checks if the left and right Flowable either match (by their corresponding bases) or there is a \
        transformation (called selector) to make them match
        """

        super().__init__()

        self._left = left
        self._right = right
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        result = left_subscription.info.get_selectors(right_subscription.info, subscriber=subscriber)

        # The resulting join_flowables Flowable propagates selectors from left and right downstream if the bases of
        # left and right Flowable match
        if isinstance(result, BaseSelectorsAndSelectorMaps):
            if isinstance(result.left, IdentitySelectorMap) and isinstance(result.right, IdentitySelectorMap):
                base = left_subscription.info.base

                selectors = {}
                if left_subscription.info.selectors is not None:
                    selectors = {**selectors, **left_subscription.info.selectors}
                if right_subscription.info.selectors is not None:
                    selectors = {**selectors, **right_subscription.info.selectors}
            else:
                base = None
                selectors = None
        else:
            base = None
            selectors = None

        observable = Zip2Observable(
            left=left_subscription.observable,
            right=right_subscription.observable,
            selector=self._func,
        )

        return Subscription(
            info=BaseAndSelectors(base=base, selectors=selectors),
            observable=observable,
        )
