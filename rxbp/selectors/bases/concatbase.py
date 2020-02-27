from typing import Iterable, List

from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.baseandselectors import BaseAndSelectors, BaseSelectorsAndSelectorMaps
from rxbp.selectors.selectionop import merge_selectors
from rxbp.selectors.selectormap import SelectorMap, IdentitySelectorMap, ObservableSelectorMap
from rxbp.subscriber import Subscriber


class ConcatBase(Base):
    """ A concat base can be thought of a list of BaseSelectors tuples """

    def __init__(
            self,
            underlying: List[BaseAndSelectors],
            sources: List[Observable],
    ):
        self.underlying = underlying
        self.sources = sources

    def get_name(self):
        def gen_names():
            for info in self.underlying:
                base = info.base
                if isinstance(base, Base):
                    yield base.get_name()
                else:
                    yield str(base)

        source_names = ', '.join(list(gen_names()))
        return f'ConcatBase({source_names})'

    def get_base_and_selector_maps(self, other: Base, subscriber: Subscriber):
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    yield left.get_selectors(right, subscriber)
            selector_results = list(gen_selectors())

            if all(isinstance(result, BaseSelectorsAndSelectorMaps) for result in selector_results):
                typed_results: List[BaseSelectorsAndSelectorMaps] = selector_results

                left_selectors, right_selectors, bases = zip(*((result.left, result.right, result.base_selectors) for result in typed_results))

                def gen_observables(selectors: List[SelectorMap], sources: List[Observable]):
                    assert len(selectors) == len(sources)

                    for selector, source in zip(selectors, sources):
                        if isinstance(selector, IdentitySelectorMap):
                            yield source, source

                        elif isinstance(selector, ObservableSelectorMap):
                            yield selector.observable, merge_selectors(source, selector.observable, subscriber.scheduler)

                if all(isinstance(selector, IdentitySelectorMap) for selector in left_selectors):
                    base = self
                    left_selector = IdentitySelectorMap()
                else:
                    sources, sources2 = zip(*gen_observables(left_selectors, self.sources))

                    base = ConcatBase(
                        bases,
                        sources=sources2
                    )
                    left_selector = ObservableSelectorMap(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                if all(isinstance(selector, IdentitySelectorMap) for selector in right_selectors):
                    base = other
                    right_selector = IdentitySelectorMap()
                else:
                    sources, _ = zip(*gen_observables(right_selectors, other.sources))

                    right_selector = ObservableSelectorMap(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                return BaseAndSelectorMaps(
                    left=left_selector,
                    right=right_selector,
                    base=base,
                )
            else:
                return None
        else:
            return None
