from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import merge_selectors
from rxbp.observables.controlledzipobservable import ControlledZipObservable
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
        if left.base is None:
            left_selectable_bases = left.selectable_bases
        else:
            left_selectable_bases = left.selectable_bases | {left.base}

        if right.base is None:
            right_selectable_bases = right.selectable_bases
        else:
            right_selectable_bases = right.selectable_bases | {right.base}

        # the base becomes anonymous after control zipping
        # use `use_base` operator to manually define a new base
        base = None

        super().__init__(base=base, selectable_bases=left_selectable_bases | right_selectable_bases)

        self._left = left
        self._right = right
        self._request_left = request_left if request_left is not None else lambda _, __: True
        self._request_right = request_right if request_right is not None else lambda _, __: True
        self._match_func = match_func if match_func is not None else lambda _, __: True

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        """
        1) subscribe to upstream flowables
        2) create ControlledZipObservable which provides a left_selector and right_selector observable
        3) extend all upstream selectors with left_selector and right_selector
        """

        # 1) subscribe to upstream flowables
        left_obs, left_selectors = self._left.unsafe_subscribe(subscriber=subscriber)
        right_obs, right_selectors = self._right.unsafe_subscribe(subscriber=subscriber)

        # 2) create ControlledZipObservable
        observable = ControlledZipObservable(
            left=left_obs, right=right_obs, request_left=self._request_left,
            request_right=self._request_right, match_func=self._match_func, scheduler=subscriber.scheduler)

        # 3.a) extend all upstream (left) selectors with left_selector
        def gen_left_merged_selector():
            for base, selector in left_selectors.items():
                yield base, merge_selectors(selector, observable.left_selector,
                                            scheduler=subscriber.scheduler)

        left_selectors = dict(gen_left_merged_selector())

        if self._left.base is not None:
            left_selectors_ = {**left_selectors, **{self._left.base: observable.left_selector}}
        else:
            left_selectors_ = left_selectors

        # 3.b) extend all upstream (right) selectors with right_selector
        def gen_right_merged_selector():
            for base, selector in right_selectors.items():
                yield base, merge_selectors(selector, observable.right_selector,
                                            scheduler=subscriber.scheduler)

        right_selectors = dict(gen_right_merged_selector())

        if self._right.base is not None:
            right_selectors_ = {**right_selectors, **{self._right.base: observable.right_selector}}
        else:
            right_selectors_ = right_selectors

        return observable, {**left_selectors_, **right_selectors_}
