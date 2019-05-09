from typing import Callable, Any, Set

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.testing.debugobservable import DebugObservable


class ZipFlowable(FlowableBase):
    def __init__(self, left: FlowableBase, right: FlowableBase,
                 selector: Callable[[Any, Any], Any] = None, auto_match: bool = None):
        has_left_base = isinstance(left.base, Base)
        has_right_base = isinstance(right.base, Base)
        # print('has_left_base', has_left_base)
        # print('has_right_base', has_right_base)

        enforce_auto_match = auto_match if isinstance(auto_match, bool) else False
        # print('enforce_auto_match', enforce_auto_match)

        def gen_matching_bases(base: Base, selectable_bases: Set[Base]):
            for other in selectable_bases:
                is_matching = base.is_matching(other)

                if is_matching:
                    sel_auto_match = base.sel_auto_match(other)

                    # print('is_maching', is_matching)
                    # print('sel_auto_match', sel_auto_match)

                    if (sel_auto_match and auto_match is None) or enforce_auto_match:
                        yield other
                        break
            yield None

        def gen_selectable_bases():
            for _ in range(1):
                if has_left_base and has_right_base:
                    is_matching = left.base.is_matching(right.base)

                    # print('bases', val1, val2)
                    if is_matching:
                        # print('sel boht bases')
                        yield left.selectable_bases | right.selectable_bases, None, None, left.base
                        break

                if has_left_base:
                    other_base = next(gen_matching_bases(left.base, selectable_bases=right.selectable_bases))
                    if other_base is not None:
                        # print('sel left base')
                        yield right.selectable_bases, other_base, None, right.base
                        break

                if has_right_base:
                    # print('has right base')
                    other_base = next(gen_matching_bases(right.base, selectable_bases=left.selectable_bases))
                    if other_base is not None:
                        # print('sel right base')
                        yield left.selectable_bases, None, other_base, left.base
                        break

                # print('left.selectable_bases', left.selectable_bases)
                # print('right.selectable_bases', right.selectable_bases)
                # print('inter_base', inter_base)

                if enforce_auto_match:
                    raise AssertionError('flowable do not match')

                yield {}, None, None, None

        selectable_bases, sel_left_base, sel_right_base, sel_base = next(gen_selectable_bases())

        # print('sel_base', sel_base)
        # print('sel_left_base', sel_left_base)
        # print('sel_right_base', sel_right_base)

        self._left = left
        self._right = right
        self._result_selector = selector
        self._sel_left_base = sel_left_base
        self._sel_right_base = sel_right_base
        base = sel_left_base or sel_right_base

        super().__init__(base=sel_base, selectable_bases=selectable_bases)

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

        if self._sel_right_base is not None:
            index_obs = left_selectors[self._sel_right_base]
            right_obs_ = select_observable(right_obs, index_obs, scheduler=subscriber.scheduler)
        else:
            selectors = {**selectors, **right_selectors}
            right_obs_ = right_obs

        obs = Zip2Observable(left=left_obs_, right=right_obs_, selector=self._result_selector)
        return obs, selectors
