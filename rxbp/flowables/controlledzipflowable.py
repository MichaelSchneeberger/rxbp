from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.selectionop import merge_selectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ControlledZipFlowable(FlowableBase):
    def __init__(
            self,
            left: FlowableBase,
            right: FlowableBase,
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
        """
        1) subscribe to upstream flowables
        2) create ControlledZipObservable which provides a left_selector and right_selector observable
        3) share_flowable all upstream selectors with left_selector and right_selector
        """

        # 1) subscribe to upstream flowables
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        # print(left_subscription.info.base)

        # 2) create ControlledZipObservable
        observable = ControlledZipObservable(
            left=left_subscription.observable,
            right=right_subscription.observable,
            request_left=self._request_left,
            request_right=self._request_right,
            match_func=self._match_func,
            scheduler=subscriber.scheduler,
        )

        # 3.a) share_flowable all upstream (left) selectors with left_selector
        def gen_merged_selector(info: BaseAndSelectors, current_selector: Observable):
            if info.selectors is not None:
                for base, selector in info.selectors.items():
                    selector = merge_selectors(
                        selector,
                        current_selector,
                        scheduler=subscriber.scheduler,
                    )
                    yield base, selector

            if info.base is not None:
                yield info.base, current_selector

        left_selectors = dict(gen_merged_selector(
            left_subscription.info,
            observable.left_selector,
        ))

        right_selectors = dict(gen_merged_selector(
            right_subscription.info,
            observable.right_selector,
        ))

        return Subscription(
            BaseAndSelectors(base=None, selectors={**left_selectors, **right_selectors}),
            observable=observable,
        )
