from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.observables.zipobservable import ZipObservable
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors, FlowableBaseAndSelectorsMatch
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class ZipIndexedFlowable(IndexedFlowableMixin):
    left: IndexedFlowableMixin
    right: IndexedFlowableMixin
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self.left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self.right.unsafe_subscribe(subscriber=subscriber)

        result = left_subscription.index.match_with(
            right_subscription.index,
            subscriber=subscriber,
            stack=self.stack,
        )

        # The resulting zip Flowable propagates selectors from left and right downstream if the bases of
        # left and right Flowable match
        if isinstance(result, FlowableBaseAndSelectorsMatch):
            if isinstance(result.left, IdentitySeqMapInfo) and isinstance(result.right, IdentitySeqMapInfo):
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
            stack=self.stack,
        )

        return left_subscription.copy(
            index=FlowableBaseAndSelectors(base=base, selectors=selectors),
            observable=observable,
        )
