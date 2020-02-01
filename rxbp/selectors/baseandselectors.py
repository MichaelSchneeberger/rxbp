from dataclasses import dataclass, field
from typing import Optional, Dict, List

from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.maptoiteratorobservable import MapToIteratorObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.matchopresult import SelectorMaps
from rxbp.selectors.selectionmsg import select_completed, select_next, SelectNext, SelectCompleted
from rxbp.selectors.selectionop import merge_selectors
from rxbp.selectors.selectormap import ObservableSelectorMap, IdentitySelectorMap
from rxbp.subscriber import Subscriber


@dataclass
class BaseSelectorsAndSelectorMaps(SelectorMaps):
    base_selectors: 'BaseAndSelectors'


@dataclass(frozen=True)
class BaseAndSelectors:
    """
    Flowables are equipped with a base and a set of selectors. Operators like `filter` add selectors to the set
    of selectors. Selectors are used create a selector_map that maps a Flowable sequence associated with some base
    into another Flowable sequence associated to another base. Matching two Flowable sequences involves one of
    the following operations:
    
        a) two identity selector_maps
        b) one identity selector map and one non-identity selector map
        c) two non-identity selector maps

    Which of this cases it will be depends on the base and selectors associated with the two Flowables. It is
    either determined by:

        1) the bases of the two Flowables
        2) the base of one Flowable and the selectors of the other
        3) the selectors of both Flowables

    The following transitions are possible:
    1 -> a
    1 -> b
    1 -> c
    2 -> b
    2 -> c
    3 -> c
    """
    
    # the base associates with the nature of the Flowable sequence, e.g. it determines
    # for instance the number of elements in the sequence
    base: Optional[Base]
    
    # selectors create "selectormaps" that map a Flowable sequence defined for a selector to
    # sequence associated to some other base
    selectors: Optional[Dict[Base, Observable]] = field(hash=False, default=None)

    def __post_init__(self):
        assert self.base is None or isinstance(self.base, Base), f'base {self.base} needs to be of type Optional[Base]'

    def get_selector_maps(
            self,
            other: 'BaseAndSelectors',
            subscriber: Subscriber,
    ):
        """
        Two possible scenarios:
        (a) is there a selector that maps the Flowable sequence associated with the left base
            to the right base.
        (b) Or, does there exist two selector maps that map both Flowable sequences to a new unknown base.
        """

        if other.base is None or self.selectors is None:
            return None

        # iterate over selectors
        for selector_base, selector_obs in self.selectors.items():

            # get selector maps of bases
            result = other.base.get_base_and_selector_maps(selector_base, subscriber=subscriber)

            if isinstance(result, BaseAndSelectorMaps):

                # print(self.selectors)
                # print(result.right)

                # the new base is the same as the old one only, if that Flowable sequence
                # is untouched.
                if isinstance(result.right, IdentitySelectorMap):

                    # extend right selector with the observable that maps the selector_base to base
                    if isinstance(result.left, ObservableSelectorMap):
                        selector_map = ObservableSelectorMap(merge_selectors(
                            result.left.observable,
                            selector_obs,
                            scheduler=subscriber.scheduler,
                        ))

                        if other.selectors is not None:
                            def gen_new_selectors():
                                for key, val in other.selectors.items():
                                    selector = merge_selectors(
                                        left=merge_selectors(
                                            left=val,
                                            right=result.left.observable,
                                            scheduler=subscriber.scheduler,
                                        ),
                                        right=selector_obs,
                                        scheduler=subscriber.scheduler,
                                    )
                                    yield key, selector
                            selectors = dict(gen_new_selectors())

                        else:
                            selectors = None

                    elif isinstance(result.left, IdentitySelectorMap):
                        selector_map = ObservableSelectorMap(selector_obs)

                        if other.selectors is not None:
                            def gen_new_selectors():
                                for key, val in other.selectors.items():
                                    selector = merge_selectors(
                                        left=val,
                                        right=selector_obs,
                                        scheduler=subscriber.scheduler,
                                    )
                                    yield key, selector
                            selectors = dict(gen_new_selectors())

                        else:
                            selectors = None

                    else:
                        raise Exception(f'illegal result "{result.left}"')

                    if selectors is not None or self.selectors is not None:
                        if selectors is not None:
                            if self.selectors is not None:
                                selectors = {**self.selectors, **selectors}
                            else:
                                selectors = selectors
                        else:
                            selectors = self.selectors

                    return self.base, selectors, result.right, selector_map

        return None

    def get_selectors(
            self,
            other: 'BaseAndSelectors',
            subscriber: Subscriber,
    ) -> Optional[BaseSelectorsAndSelectorMaps]:

        # bases are of type Optional[Base], therefore check first if base is not None
        if self.base is not None and other.base is not None:

            result = self.base.get_base_and_selector_maps(other.base, subscriber=subscriber)

            # this BaseAndSelectors and the other BaseAndSelectors match directly with
            # their bases
            if isinstance(result, BaseAndSelectorMaps):

                # the selectors can be taken over to the new base selector tuple
                if self.selectors is not None and other.selectors is not None:
                    if self.selectors is not None:
                        if other.selectors is None:
                            selectors = self.selectors
                        else:
                            selectors = {**self.selectors, **other.selectors}
                    else:
                        selectors = other.selectors
                else:
                    selectors = {}

                return BaseSelectorsAndSelectorMaps(
                    base_selectors=BaseAndSelectors(
                        base=result.base,
                        selectors=selectors,
                    ),
                    left=result.left,
                    right=result.right,
                )

        result = self.get_selector_maps(
            other=other,
            subscriber=subscriber,
        )

        if result is not None:
            base, selectors, left_selector_map, right_selector_map = result

            return BaseSelectorsAndSelectorMaps(
                left=left_selector_map,
                right=right_selector_map,
                base_selectors=BaseAndSelectors(
                    base=base,
                    selectors=selectors,
                ),
            )

        result = other.get_selector_maps(
            other=self,
            subscriber=subscriber,
        )

        if result is not None:
            base, selectors, right_selector_map, left_selector_map = result

            return BaseSelectorsAndSelectorMaps(
                left=left_selector_map,
                right=right_selector_map,
                base_selectors=BaseAndSelectors(
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

                    result = sel_base_1.get_base_and_selector_maps(sel_base_2, subscriber=subscriber)

                    # if two bases match ...
                    if isinstance(result, BaseAndSelectorMaps):

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

                        if isinstance(result.left, IdentitySelectorMap) and isinstance(result.right, IdentitySelectorMap):
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

                            left_sel = RefCountObservable(MapToIteratorObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: isinstance(t[0], SelectNext),
                                    scheduler=subscriber.scheduler,
                                ),
                                func=left_selector,
                            ), subject=PublishOSubject(scheduler=subscriber.scheduler))

                            right_sel = RefCountObservable(MapToIteratorObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: isinstance(t[1], SelectNext),
                                    scheduler=subscriber.scheduler,
                                ),
                                func=right_selector,
                            ), subject=PublishOSubject(scheduler=subscriber.scheduler))

                            right_selector_map = MapObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: type(t[0]) == type(t[1]),
                                    scheduler=subscriber.scheduler,
                                ),
                                selector=lambda t: t[0],
                            )

                            return BaseSelectorsAndSelectorMaps(
                                left=ObservableSelectorMap(left_sel),
                                right=ObservableSelectorMap(right_sel),
                                base_selectors=BaseAndSelectors(
                                    base=None,
                                    selectors={
                                        sel_base_1: right_selector_map,
                                        **{k: merge_selectors(v, left_sel, scheduler=subscriber.scheduler) for k, v in
                                           self.selectors.items() if k != sel_base_1},
                                        **{k: merge_selectors(v, right_sel, scheduler=subscriber.scheduler) for k, v in
                                           other.selectors.items() if k != sel_base_2}
                                    },
                                ),
                            )

        return None
