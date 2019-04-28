from typing import Callable, Any, Set

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber


class ZipFlowable(FlowableBase):
    def __init__(self, left: FlowableBase, right: FlowableBase,
                 selector: Callable[[Any, Any], Any] = None, auto_match: bool = None):
        has_left_base = isinstance(left.base, Base)
        has_right_base = isinstance(right.base, Base)

        enforce_auto_match = auto_match if isinstance(auto_match, bool) else False

        def gen_matching_bases(base: Base, selectable_bases: Set[Base]):
            for other in selectable_bases:
                val1, val2 = base.is_matching(other)
                if val1 and ((val2 and auto_match is None) or enforce_auto_match):
                    yield other
                    break
            yield None

        def gen_selectable_bases():
            for _ in range(1):
                if has_left_base and has_right_base:
                    val1, _ = left.base.is_matching(right.base)
                    if val1:
                        yield left.selectable_bases | right.selectable_bases, None, None
                        break

                if has_left_base:
                    other_base = next(gen_matching_bases(left.base, selectable_bases=right.selectable_bases))
                    if other_base is not None:
                        yield right.selectable_bases, other_base, None
                        break

                if has_right_base:
                    other_base = next(gen_matching_bases(right.base, selectable_bases=left.selectable_bases))
                    if other_base is not None:
                        yield left.selectable_bases, None, other_base
                        break

                if enforce_auto_match:
                    raise AssertionError('flowable do not match')

                yield {}, None, None

        selectable_bases, sel_left_base, sel_right_base = next(gen_selectable_bases())

        self._left = left
        self._right = right
        self._result_selector = selector
        self._sel_left_base = sel_left_base
        self._sel_right_base = sel_right_base

        super().__init__(selectable_bases=selectable_bases)

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        left_obs, left_selectors = self._left.unsafe_subscribe(subscriber=subscriber)
        right_obs, right_selectors = self._right.unsafe_subscribe(subscriber=subscriber)

        selectors = {}

        if self._sel_left_base is not None:
            index_obs = right_selectors[self._sel_left_base]
            left_obs_ = select_observable(left_obs, index_obs, scheduler=subscriber.scheduler)
        else:
            selectors = {**selectors, **left_selectors}
            left_obs_ = left_obs

        if self._sel_right_base:
            index_obs = left_selectors[self._sel_right_base]
            right_obs_ = select_observable(right_obs, index_obs, scheduler=subscriber.scheduler)
        else:
            selectors = {**selectors, **right_selectors}
            right_obs_ = right_obs

        obs = Zip2Observable(left=left_obs_, right=right_obs_, selector=self._result_selector)
        return obs, selectors
