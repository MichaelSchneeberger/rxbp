from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.internal.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.subscriber import Subscriber


class ZipFlowable(FlowableBase):
    def __init__(self, left: FlowableBase, right: FlowableBase,
                 selector: Callable[[Any, Any], Any] = None, auto_match: bool = None):

        auto_match_ = auto_match if isinstance(auto_match, bool) else True
        selectable_bases = set()

        if auto_match_ is True:
            if left.base is not None and left.base == right.base:
                transform_left = False
                transform_right = False

                selectable_bases = left.selectable_bases | right.selectable_bases

            elif left.base is not None and left.base in right.selectable_bases:
                transform_left = True
                transform_right = False

                selectable_bases = right.selectable_bases

            elif right.base is not None and right.base in left.selectable_bases:
                transform_left = False
                transform_right = True

                selectable_bases = left.selectable_bases

            else:

                raise AssertionError('flowable do not match')

        else:
            transform_left = False
            transform_right = False

        self._left = left
        self._right = right
        self._result_selector = selector
        self._transform_left = transform_left
        self._transform_right = transform_right

        super().__init__(selectable_bases=selectable_bases)

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        left_obs, left_selectors = self._left.unsafe_subscribe(subscriber=subscriber)
        right_obs, right_selectors = self._right.unsafe_subscribe(subscriber=subscriber)

        selectors = {}

        if self._transform_left:
            index_obs = right_selectors[self._left.base]
            left_obs_ = select_observable(left_obs, index_obs, scheduler=subscriber.scheduler)
        else:
            selectors = {**selectors, **left_selectors}
            left_obs_ = left_obs

        if self._transform_right:
            index_obs = left_selectors[self._right.base]
            right_obs_ = select_observable(right_obs, index_obs, scheduler=subscriber.scheduler)
        else:
            selectors = {**selectors, **right_selectors}
            right_obs_ = right_obs

        obs = Zip2Observable(left=left_obs_, right=right_obs_, selector=self._result_selector)
        return obs, selectors
