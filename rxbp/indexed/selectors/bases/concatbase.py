from dataclasses import dataclass
from traceback import FrameSummary
from typing import List, Tuple

from rxbp.indexed.selectors.selectionop import merge_selectors
from rxbp.indexed.selectors.observableseqmapinfo import ObservableSeqMapInfo
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.indexed.selectors.seqmapinfo import SeqMapInfo
from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.indexed.selectors.flowablebase import FlowableBase, FlowableBaseMatch
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors, FlowableBaseAndSelectorsMatch
from rxbp.subscriber import Subscriber


class ConcatBase(FlowableBase):
    """ A concat base can be thought of a list of BaseSelectors tuples """

    def __init__(
            self,
            underlying: Tuple[FlowableBaseAndSelectors],
            sources: Tuple[Observable],
    ):
        self.underlying = underlying
        self.sources = sources

    def get_name(self) -> str:
        def gen_names():
            for info in self.underlying:
                base = info.base
                if isinstance(base, FlowableBase):
                    yield base.get_name()
                else:
                    yield str(base)

        source_names = ', '.join(list(gen_names()))
        return f'ConcatBase({source_names})'

    def match_with(
            self,
            other: FlowableBase,
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ):
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    yield left.match_with(
                        other=right,
                        subscriber=subscriber,
                        stack=stack,
                    )
            selector_results = list(gen_selectors())

            if all(isinstance(result, FlowableBaseAndSelectorsMatch) for result in selector_results):
                typed_results: List[FlowableBaseAndSelectorsMatch] = selector_results

                left_selectors, right_selectors, bases = zip(*((result.left, result.right, result.base_selectors) for result in typed_results))

                def gen_observables(selectors: List[SeqMapInfo], sources: List[Observable]):
                    assert len(selectors) == len(sources)

                    for selector, source in zip(selectors, sources):
                        if isinstance(selector, IdentitySeqMapInfo):
                            yield source, source

                        elif isinstance(selector, ObservableSeqMapInfo):
                            yield selector.observable, merge_selectors(
                                source,
                                selector.observable,
                                subscriber.scheduler,
                                stack=stack,
                            )

                if all(isinstance(selector, IdentitySeqMapInfo) for selector in left_selectors):
                    base = self
                    left_selector = IdentitySeqMapInfo()
                else:
                    sources, sources2 = zip(*gen_observables(left_selectors, self.sources))

                    base = ConcatBase(
                        bases,
                        sources=sources2
                    )
                    left_selector = ObservableSeqMapInfo(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                if all(isinstance(selector, IdentitySeqMapInfo) for selector in right_selectors):
                    base = other
                    right_selector = IdentitySeqMapInfo()
                else:
                    sources, _ = zip(*gen_observables(right_selectors, other.sources))

                    right_selector = ObservableSeqMapInfo(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                return FlowableBaseMatch(
                    left=left_selector,
                    right=right_selector,
                    base=base,
                )
            else:
                return None
        else:
            return None

    # def get_name(self):
    #     def gen_names():
    #         for info in self.underlying:
    #             base = info.base
    #             if isinstance(base, Base):
    #                 yield base.get_name()
    #             else:
    #                 yield str(base)
    #
    #     source_names = ', '.join(list(gen_names()))
    #     return f'ConcatBase({source_names})'
