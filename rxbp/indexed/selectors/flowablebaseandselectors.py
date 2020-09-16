from dataclasses import dataclass, field
from traceback import FrameSummary
from typing import Optional, Dict, Tuple, List

from rxbp.indexed.selectors.flowablebase import FlowableBase, FlowableBaseMatch
from rxbp.indexed.selectors.seqmapinfo import SeqMapInfo
from rxbp.indexed.selectors.seqmapinfopair import SeqMapInfoPair
from rxbp.indexed.selectors.selectnext import SelectNext, select_next
from rxbp.indexed.selectors.selectcompleted import select_completed, SelectCompleted
from rxbp.indexed.selectors.selectionop import merge_selectors
from rxbp.indexed.selectors.observableseqmapinfo import ObservableSeqMapInfo
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.debugobservable import DebugObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.init.initdebugobservable import init_debug_observable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.maptoiteratorobservable import MapToIteratorObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.subscriber import Subscriber


@dataclass
class FlowableBaseAndSelectorsMatch(SeqMapInfoPair):
    # base and selectors for the new Flowable
    base_selectors: 'FlowableBaseAndSelectors'  # rename to flowable_base


@dataclass
class FlowableBaseAndSelectors:
    """
    Flowables are equipped with a base and a set of selectors. Operators like `filter` or 'controlled_zip'
    extend the existing selectors and add new selectors as well. The selectors are the capability to map
    another Flowable sequence to matches the current one. Two Flowable sequences match if
    zipping the two sequences makes sense.

    Matching two Flowable sequences involves one of the following operations:
    
        a) two identity selector_maps
        b) one identity selector map and one non-identity selector map
        c) two non-identity selector maps

    Which operation is selected depends on:

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
    
    # the base determines the order and number of elements in the Flowable sequence
    base: Optional[FlowableBase]
    
    # selector is the capability to map another Flowable sequence to match the current one.
    selectors: Optional[Dict[FlowableBase, Observable]] = field(hash=False, default=None)

    def __post_init__(self):
        assert self.base is None or isinstance(self.base, FlowableBase), \
            f'base {self.base} needs to be of type Optional[Base]'

    def get_selector_maps(
            self,
            other: 'FlowableBaseAndSelectors',
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ) -> Optional[Tuple['FlowableBaseAndSelectors', SeqMapInfo, SeqMapInfo]]:
        """
        Two possible scenarios:
        (1) is there a selector that maps the Flowable sequence associated with the left base
            to the right base.
        (2) Or, does there exist two selector maps that map both Flowable sequences to a new unknown base.
        """

        if other.base is None or self.selectors is None:
            return None

        # iterate over selectors
        for selector_base, selector_obs in self.selectors.items():

            # get selector maps of bases
            result = other.base.match_with(selector_base, subscriber=subscriber, stack=stack)

            if isinstance(result, FlowableBaseMatch):

                # the new base is the same as the old one only, if that Flowable sequence
                # is untouched.
                if isinstance(result.right, IdentitySeqMapInfo):

                    # extend right selector with the observable that maps the selector_base to base
                    if isinstance(result.left, ObservableSeqMapInfo):
                        selector_map = ObservableSeqMapInfo(merge_selectors(
                            result.left.observable,
                            selector_obs,
                            subscribe_scheduler=subscriber.scheduler,
                            stack=stack,
                        ))

                        if other.selectors is not None:
                            def gen_new_selectors():
                                for key, val in other.selectors.items():
                                    selector = merge_selectors(
                                        left=merge_selectors(
                                            left=val,
                                            right=result.left.observable,
                                            subscribe_scheduler=subscriber.scheduler,
                                            stack=stack,
                                        ),
                                        right=selector_obs,
                                        subscribe_scheduler=subscriber.scheduler,
                                        stack=stack,
                                    )
                                    yield key, selector
                            selectors = dict(gen_new_selectors())

                        else:
                            selectors = None

                    elif isinstance(result.left, IdentitySeqMapInfo):
                        selector_map = ObservableSeqMapInfo(selector_obs)

                        if other.selectors is not None:
                            def gen_new_selectors():
                                for key, val in other.selectors.items():
                                    selector = merge_selectors(
                                        left=val,
                                        right=selector_obs,
                                        subscribe_scheduler=subscriber.scheduler,
                                        stack=stack,
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

                    return FlowableBaseAndSelectors(self.base, selectors), result.right, selector_map

        return None

    def match_with(
            self,
            other: 'FlowableBaseAndSelectors',
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ) -> Optional[FlowableBaseAndSelectorsMatch]:

        # bases are of type Optional[Base], therefore check first if base is not None
        if self.base is not None and other.base is not None:

            result = self.base.match_with(other.base, subscriber=subscriber, stack=stack)

            # this BaseAndSelectors and the other BaseAndSelectors match directly with
            # their bases
            if isinstance(result, FlowableBaseMatch):

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

                return FlowableBaseAndSelectorsMatch(
                    base_selectors=FlowableBaseAndSelectors(
                        base=result.base,
                        selectors=selectors,
                    ),
                    left=result.left,
                    right=result.right,
                )

        result = self.get_selector_maps(
            other=other,
            subscriber=subscriber,
            stack=stack,
        )

        if result is not None:
            base_selectors, left_selector_map, right_selector_map = result

            return FlowableBaseAndSelectorsMatch(
                left=left_selector_map,
                right=right_selector_map,
                base_selectors=base_selectors,
            )

        result = other.get_selector_maps(
            other=self,
            subscriber=subscriber,
            stack=stack,
        )

        if result is not None:
            base_selectors, right_selector_map, left_selector_map = result

            return FlowableBaseAndSelectorsMatch(
                left=left_selector_map,
                right=right_selector_map,
                base_selectors=base_selectors,
            )

        # two bases B1, B2 defined in selectors match, the following map is created
        # - define a new anonymous base B_match
        # - define two selectors that map a sequence with base B1 and B2 to base B_match
        #   in case B1 and B2 match with identity, only define no selector
        # - define two selectors that map the two sequences of the match operator to base B_match
        if self.selectors is not None and other.selectors is not None:

            # check if some tuple of bases matches
            for sel_base_1, sel_observable_1 in self.selectors.items():
                for sel_base_2, sel_observable_2 in other.selectors.items():

                    result = sel_base_1.match_with(
                        sel_base_2,
                        subscriber=subscriber,
                        stack=stack,
                    )

                    # if two bases match ...
                    if isinstance(result, FlowableBaseMatch):

                        # once right is completed, keep consuming left side until it is completed as well
                        def request_left(left, right):
                            return not (isinstance(left, SelectCompleted) and isinstance(right, SelectNext))

                        def request_right(left, right):
                            return not (isinstance(right, SelectCompleted) and isinstance(left, SelectNext))

                        if isinstance(result.left, IdentitySeqMapInfo) and isinstance(result.right, IdentitySeqMapInfo):
                            merge_sel = RefCountObservable(
                                source=ControlledZipObservable(
                                    left=sel_observable_1, #init_debug_observable(sel_observable_1, name='d1', stack=stack, subscriber=subscriber),
                                    right=sel_observable_2, #init_debug_observable(sel_observable_2, name='d1', stack=stack, subscriber=subscriber),
                                    request_left=request_left,
                                    request_right=request_right,
                                    match_func=lambda _, __: True,
                                    scheduler=subscriber.scheduler,
                                    stack=stack,
                                ), #name='d2', stack=stack, subscriber=subscriber),
                                subject=PublishObservableSubject(),
                                subscribe_scheduler=subscriber.subscribe_scheduler,
                                stack=stack,
                            )

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

                            left_sel = RefCountObservable(
                                source=MapToIteratorObservable(
                                    source=FilterObservable(
                                        source=merge_sel,
                                        predicate=lambda t: isinstance(t[0], SelectNext),
                                    ),
                                    func=left_selector,
                                ),
                                subject=PublishObservableSubject(),
                                subscribe_scheduler=subscriber.subscribe_scheduler,
                                stack=stack,
                            )

                            right_sel = RefCountObservable(
                                source=MapToIteratorObservable(
                                    source=FilterObservable(
                                        source=merge_sel,
                                        predicate=lambda t: isinstance(t[1], SelectNext),
                                    ),
                                    func=right_selector,
                                ),
                                subject=PublishObservableSubject(),
                                subscribe_scheduler=subscriber.scheduler,
                                stack=stack,
                            )

                            right_selector_map = MapObservable(
                                source=FilterObservable(
                                    source=merge_sel,
                                    predicate=lambda t: type(t[0]) == type(t[1]),
                                ),
                                func=lambda t: t[0],
                            )

                            return FlowableBaseAndSelectorsMatch(
                                left=ObservableSeqMapInfo(left_sel),
                                right=ObservableSeqMapInfo(right_sel),
                                base_selectors=FlowableBaseAndSelectors(
                                    base=None,
                                    selectors={
                                        sel_base_1: right_selector_map,
                                        **{k: merge_selectors(v, left_sel, subscribe_scheduler=subscriber.scheduler, stack=stack) for k, v in
                                           self.selectors.items() if k != sel_base_1},
                                        **{k: merge_selectors(v, right_sel, subscribe_scheduler=subscriber.scheduler, stack=stack) for k, v in
                                           other.selectors.items() if k != sel_base_2}
                                    },
                                ),
                            )

        return None
