from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import merge_selectors
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.subscriber import Subscriber


class ControlledZipFlowable(FlowableBase):
    def __init__(self, left: FlowableBase, right: FlowableBase,
                   request_left: Callable[[Any, Any], bool],
                   request_right: Callable[[Any, Any], bool],
                   match_func: Callable[[Any, Any], bool]):
        if left.base is None:
            left_selectable_bases = left.selectable_bases
        else:
            left_selectable_bases = left.selectable_bases | {left.base}

        if right.base is None:
            right_selectable_bases = right.selectable_bases
        else:
            right_selectable_bases = right.selectable_bases | {right.base}

        super().__init__(base=None, selectable_bases=left_selectable_bases | right_selectable_bases)

        self._left = left
        self._right = right
        self._request_left = request_left
        self._request_right = request_right
        self._match_func = match_func

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        left_obs, left_selectors = self._left.unsafe_subscribe(subscriber=subscriber)
        right_obs, right_selectors = self._right.unsafe_subscribe(subscriber=subscriber)

        observable = ControlledZipObservable(
            left=left_obs, right=right_obs, request_left=self._request_left,
            request_right=self._request_right, match_func=self._match_func, scheduler=subscriber.scheduler)

        # apply filter selector to each selector
        def gen_left_merged_selector():
            for base, indexing in left_selectors.items():
                yield base, merge_selectors(indexing, observable.left_selector,
                                            scheduler=subscriber.scheduler)

        left_selectors = dict(gen_left_merged_selector())

        if self._left.base is not None:
            left_selectors_ = {**left_selectors, **{self._left.base: observable.left_selector}}
        else:
            left_selectors_ = left_selectors

        # apply filter selector to each selector
        def gen_right_merged_selector():
            for base, indexing in right_selectors.items():
                yield base, merge_selectors(indexing, observable.right_selector,
                                            scheduler=subscriber.scheduler)

        right_selectors = dict(gen_right_merged_selector())

        if self._right.base is not None:
            right_selectors_ = {**right_selectors, **{self._right.base: observable.right_selector}}
        else:
            right_selectors_ = right_selectors

        # print('left_selectors_', left_selectors_)
        # print('right_selectors_', right_selectors_)

        return observable, {**left_selectors_, **right_selectors_}
