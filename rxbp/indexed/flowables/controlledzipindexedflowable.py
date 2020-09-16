from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.init.initindexedsubscription import init_indexed_subscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.observables.controlledzipindexedobservable import ControlledZipIndexedObservable
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.indexed.selectors.selectionop import merge_selectors
from rxbp.observable import Observable
from rxbp.subscriber import Subscriber


@dataclass
class ControlledZipIndexedFlowable(IndexedFlowableMixin):
    left: IndexedFlowableMixin
    right: IndexedFlowableMixin
    request_left: Callable[[Any, Any], bool]
    request_right: Callable[[Any, Any], bool]
    match_func: Callable[[Any, Any], bool]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        """
        1) subscribe to upstream flowables
        2) create ControlledZipObservable which provides a left_selector and right_selector observable
        3) share_flowable all upstream selectors with left_selector and right_selector
        """

        # 1) subscribe to upstream flowables
        left_subscription = self.left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self.right.unsafe_subscribe(subscriber=subscriber)

        # 2) create ControlledZipObservable
        observable = ControlledZipIndexedObservable(
            left=left_subscription.observable,
            right=right_subscription.observable,
            request_left=self.request_left,
            request_right=self.request_right,
            match_func=self.match_func,
            scheduler=subscriber.scheduler,
        )

        # 3.a) share_flowable all upstream (left) selectors with left_selector
        def gen_merged_selector(info: FlowableBaseAndSelectors, current_selector: Observable):
            if info.selectors is not None:
                for base, selector in info.selectors.items():
                    selector = merge_selectors(
                        selector,
                        current_selector,
                        subscribe_scheduler=subscriber.scheduler,
                        stack=self.stack
                    )
                    yield base, selector

            if info.base is not None:
                yield info.base, current_selector

        left_selectors = dict(gen_merged_selector(
            left_subscription.index,
            observable.left_selector,
        ))

        right_selectors = dict(gen_merged_selector(
            right_subscription.index,
            observable.right_selector,
        ))

        return init_indexed_subscription(
            index=FlowableBaseAndSelectors(base=None, selectors={**left_selectors, **right_selectors}),
            observable=observable,
        )
