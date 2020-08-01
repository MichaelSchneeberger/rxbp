from typing import Callable, Any

from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.observables.zipobservable import ZipObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors, BaseSelectorsAndSelectorMaps
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class IndexedZipFlowable(IndexedFlowableMixin):
    def __init__(
            self,
            left: IndexedFlowableMixin,
            right: IndexedFlowableMixin,
            func: Callable[[Any, Any], Any] = None,
    ):
        super().__init__()

        self._left = left
        self._right = right
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        result = left_subscription.index.get_selectors(
            right_subscription.index,
            subscriber=subscriber,
        )

        # The resulting zip Flowable propagates selectors from left and right downstream if the bases of
        # left and right Flowable match
        if isinstance(result, BaseSelectorsAndSelectorMaps):
            if isinstance(result.left, IdentitySelectorMap) and isinstance(result.right, IdentitySelectorMap):
                base = left_subscription.index.base

                selectors = {}
                if left_subscription.index.selectors is not None:
                    selectors = {**selectors, **left_subscription.index.selectors}
                if right_subscription.index.selectors is not None:
                    selectors = {**selectors, **right_subscription.index.selectors}
            else:
                base = None
                selectors = None
        else:
            base = None
            selectors = None

        observable = ZipObservable(
            left=left_subscription.observable,
            right=right_subscription.observable,
            selector=self._func,
        )

        return left_subscription.copy(
            index=BaseAndSelectors(base=base, selectors=selectors),
            observable=observable,
        )
