from typing import Callable, Any, Set

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.testing.debugobservable import DebugObservable


class ZipFlowable(FlowableBase):
    def __init__(
            self,
            left: FlowableBase,
            right: FlowableBase,
            selector: Callable[[Any, Any], Any] = None,
            auto_match: bool = None
    ):
        """
        :param left:
        :param right:
        :param selector:
        :param auto_match: if set to False then this Flowable works like a normal zip operation, if set to False then \
        it checks if the left and right Flowable either match (by their corresponding bases) or there is a \
        transformation (called selector) to make them match
        """

        has_left_base = isinstance(left.base, Base)
        has_right_base = isinstance(right.base, Base)

        enforce_auto_match = auto_match if isinstance(auto_match, bool) else False

        def gen_matching_bases(base: Base, selectable_bases: Set[Base]):
            for other in selectable_bases:
                is_matching = base.is_matching(other)

                if is_matching:
                    sel_auto_match = base.sel_auto_match(other)

                    if (sel_auto_match and auto_match is None) or enforce_auto_match:
                        yield other
                        break
            yield None

        def gen_selectable_bases():
            """ Generates 4 values
            1) the base of this Flowable
            2) a set of bases for which a selector exists that transforms the Flowable to match
               the basis of this Flowable (not used for the auto_match of this zip Flowable, but possibly used for
               auto_match in downstream Flowables)
            3) (used only if auto_match==True) select left base transformation
            4) (used only if auto_match==True) select right base transformation

            raise Exception auto_match==True but bases don't match and no transformation was was found
            """

            for _ in range(1):
                if has_left_base and has_right_base:
                    is_matching = left.base.is_matching(right.base)

                    if is_matching:
                        yield left.base, left.selectable_bases | right.selectable_bases, None, None
                        break

                if has_left_base:
                    other_base = next(gen_matching_bases(left.base, selectable_bases=right.selectable_bases))
                    if other_base is not None:
                        yield right.base, right.selectable_bases, other_base, None
                        break

                if has_right_base:
                    other_base = next(gen_matching_bases(right.base, selectable_bases=left.selectable_bases))
                    if other_base is not None:
                        yield left.base, left.selectable_bases, None, other_base
                        break

                if enforce_auto_match:
                    raise AssertionError('flowable do not match')

                yield None, {}, None, None

        sel_base, selectable_bases, sel_left_base, sel_right_base = next(gen_selectable_bases())

        self._left = left
        self._right = right
        self._result_selector = selector
        self._sel_left_base = sel_left_base
        self._sel_right_base = sel_right_base

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
