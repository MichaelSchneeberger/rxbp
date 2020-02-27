from dataclasses import dataclass
from typing import Optional, Dict

from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.maptolistobservable import MapToListObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.selectors.base import Base
from rxbp.selectors.matchopresult import MatchOpResultMixin
from rxbp.selectors.selectionmsg import select_completed, select_next, SelectNext, SelectCompleted
from rxbp.selectors.selectionop import merge_selectors
from rxbp.selectors.selector import ObservableSelector, IdentitySelector
from rxbp.subscriber import Subscriber


@dataclass
class BaseSelectorsTuple:
    base: Optional[Base]
    selectors: Optional[Dict[Base, Observable]] = None

    @dataclass
    class MatchedBaseMapping(MatchOpResultMixin):
        base_selectors: 'BaseSelectorsTuple'

    def get_selectors(
            self,
            other: 'BaseSelectorsTuple',
            subscriber: Subscriber,
    ) -> Optional['BaseSelectorsTuple.MatchedBaseMapping']:

        # bases are of type Optional[Base], therefore check first base is not None
        if self.base is not None and other.base is not None:
            result = self.base.get_selectors(other.base, subscriber=subscriber)

            if isinstance(result, Base.MatchedBaseMapping):
                if self.selectors is not None and other.selectors is not None:

                    if self.selectors is not None:
                        if other.selectors is None:
                            selectors = self.selectors
                        else:
                            selectors = {**self.selectors, **other.selectors}
                    else:
                        selectors = other.selectors

                else:
                    selectors = None

                return BaseSelectorsTuple.MatchedBaseMapping(
                    base_selectors=BaseSelectorsTuple(
                        base=result.base,
                        selectors=selectors,
                    ),
                    left=result.left,
                    right=result.right,
                )

        def find_selectors(
                base: Base,
                selectors: Optional[Dict[Base, Observable]],
                other_selectors: Dict[Base, Observable],
        ):
            """ is there a selector that transforms the base to some other base that can then be matched """

            for selector_base, selector_observable in other_selectors.items():
                result = base.get_selectors(selector_base, subscriber=subscriber)

                if isinstance(result, Base.MatchedBaseMapping):
                    # left_sel = result.left

                    if isinstance(result.right, IdentitySelector):

                        # extend right selector with the observable that maps the selector_base to base
                        if isinstance(result.left, ObservableSelector):
                            right_sel = ObservableSelector(merge_selectors(
                                result.left.observable,
                                selector_observable,
                                scheduler=subscriber.scheduler,
                            ))

                            if selectors is not None:
                                selectors = {k: merge_selectors(merge_selectors(v, result.left.observable, scheduler=subscriber.scheduler), selector_observable, scheduler=subscriber.scheduler) for k, v
                                            in selectors.items()}

                        elif isinstance(result.left, IdentitySelector):
                            right_sel = ObservableSelector(selector_observable)

                            if selectors is not None:
                                selectors = {k: merge_selectors(v, selector_observable, scheduler=subscriber.scheduler) for k, v
                                            in selectors.items()}

                        else:
                            raise Exception(f'illegal result "{result.right}"')

                        return result.base, right_sel, selectors

            return None

        if self.base is not None and other.selectors is not None:
            result = find_selectors(
                base=self.base,
                selectors=self.selectors,
                other_selectors=other.selectors,
            )

            if result is not None:
                base, selector, selectors = result

                if selectors is not None or other.selectors is not None:
                    if selectors is not None:
                        if other.selectors is not None:
                            selectors = {**other.selectors, **selectors}
                        else:
                            selectors = selectors
                    else:
                        selectors = other.selectors

                return BaseSelectorsTuple.MatchedBaseMapping(
                    left=selector,
                    right=IdentitySelector(),
                    base_selectors=BaseSelectorsTuple(
                        base=base,
                        selectors={**other.selectors, **selectors},
                    ),
                )

        if other.base is not None and self.selectors is not None:
            result = find_selectors(
                base=other.base,
                selectors=other.selectors,
                other_selectors=self.selectors,
            )

            if result is not None:
                base, selector, selectors = result

                if selectors is not None or other.selectors is not None:
                    if selectors is not None:
                        if other.selectors is not None:
                            selectors = {**other.selectors, **selectors}
                        else:
                            selectors = selectors
                    else:
                        selectors = other.selectors

                return BaseSelectorsTuple.MatchedBaseMapping(
                    left=IdentitySelector(),
                    right=selector,
                    base_selectors=BaseSelectorsTuple(
                        base=base,
                        selectors=selectors,
                    ),
                )

        # two bases B1, B2 defined in selectors match, the following MatchedBaseMap is created
        # - define a new anonymeous base B_match
        # - define two selectors that map a sequence with base B1 and B2 to base B_match
        #   in case B1 and B2 match with identity, only define no selector
        # - define two selectors that map the two sequences of the match operator to base B_match
        if self.selectors is not None and other.selectors is not None:

            # check if some tuple of bases matches
            for sel_base_1, sel_observable_1 in self.selectors.items():
                for sel_base_2, sel_observable_2 in other.selectors.items():

                    result = sel_base_1.get_selectors(sel_base_2, subscriber=subscriber)

                    # if two bases match ...
                    if isinstance(result, Base.MatchedBaseMapping):
                        # once right is completed, keep consuming left side until it is completed as well
                        def request_left(left, right):
                            return not all([
                                isinstance(left, SelectCompleted),
                                isinstance(right, SelectNext),
                            ])

                        def request_right(left, right):
                            return not all([
                                isinstance(right, SelectCompleted),
                                isinstance(left, SelectNext),
                            ])

                        def match_func(left, right):
                            return True

                        if isinstance(result.left, IdentitySelector) and isinstance(result.right, IdentitySelector):
                            merge_sel = ControlledZipObservable(
                                left=sel_observable_1,
                                right=sel_observable_2,
                                request_left=request_left,
                                request_right=request_right,
                                match_func=match_func,
                                scheduler=subscriber.scheduler,
                            )
                            merge_sel = RefCountObservable(source=merge_sel, subject=PublishOSubject(scheduler=subscriber.scheduler))

                            def left_selector(t):
                                if isinstance(t[1], SelectNext):
                                    return [select_next, select_completed]
                                else:
                                    return [select_completed]

                            def right_selector(t):
                                if isinstance(t[0], SelectNext):
                                    return [select_next, select_completed]
                                else:
                                    return [select_completed]

                            left_sel = RefCountObservable(MapToListObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: isinstance(t[0], SelectNext),
                                    scheduler=subscriber.scheduler,
                                ),
                                selector=left_selector,
                            ), subject=PublishOSubject(scheduler=subscriber.scheduler))

                            right_sel = RefCountObservable(MapToListObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: isinstance(t[1], SelectNext),
                                    scheduler=subscriber.scheduler,
                                ),
                                selector=right_selector,
                            ), subject=PublishOSubject(scheduler=subscriber.scheduler))

                            selector = MapObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: type(t[0]) == type(t[1]),
                                    scheduler=subscriber.scheduler,
                                ),
                                selector=lambda t: t[0],
                            )

                            return BaseSelectorsTuple.MatchedBaseMapping(
                                left=ObservableSelector(left_sel),
                                right=ObservableSelector(right_sel),
                                base_selectors=BaseSelectorsTuple(
                                    base=None,
                                    selectors={
                                        sel_base_1: selector,
                                        **{k: merge_selectors(v, left_sel, scheduler=subscriber.scheduler) for k, v in
                                           self.selectors.items() if k != sel_base_1},
                                        **{k: merge_selectors(v, right_sel, scheduler=subscriber.scheduler) for k, v in
                                           other.selectors.items() if k != sel_base_2}
                                    },
                                ),
                            )

        return None