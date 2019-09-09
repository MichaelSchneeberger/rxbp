from abc import ABC, abstractmethod
from typing import Any, Iterable, List

from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.getselectormixin import IdentitySelector, SelectorResult, SelectorFound, NoSelectorFound, \
    GetSelectorMixin, ObservableSelector, Selector
from rxbp.selectors.observables.identityselectorobservable import IdentitySelectorObservable
from rxbp.subscriber import Subscriber


class Base(GetSelectorMixin, ABC):
    pass


class NumericalBase(Base):
    def __init__(self, num: int):
        self.num = num

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, NumericalBase) and self.num == other.num:
            return SelectorFound(IdentitySelector(), IdentitySelector())
        else:
            return NoSelectorFound()


class ObjectRefBase(Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return SelectorFound(IdentitySelector(), IdentitySelector())
        else:
            return NoSelectorFound()


class PairwiseBase(Base):
    def __init__(self, underlying: Base):
        self.underlying = underlying

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, PairwiseBase):
            return self.underlying.get_selectors(other.underlying, subscriber=subscriber)
        else:
            return NoSelectorFound()


class ConcatBase(Base):
    def __init__(
            self,
            underlying: Iterable[GetSelectorMixin],
            sources: List[Observable],
    ):
        self.underlying = list(underlying)
        self.sources = [IdentitySelectorObservable(source) for source in sources]

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    # print(left.base)
                    # print(right.base)
                    result = left.get_selectors(right, subscriber)
                    yield result
            results = list(gen_selectors())

            if all(isinstance(result, SelectorFound) for result in results):
                typed_results: List[SelectorFound] = results

                left_selectors, right_selectors = zip(*[(result.left, result.right) for result in typed_results])

                def gen_observables(selectors: List[Selector], sources: List[Observable]):
                    for selector, source in zip(selectors, sources):
                        if isinstance(selector, IdentitySelector):
                            yield source
                        elif isinstance(selector, ObservableSelector):
                            yield selector.observable

                # print(left_selectors)
                # print(right_selectors)

                if all(isinstance(selector, IdentitySelector) for selector in left_selectors):
                    left_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(left_selectors, self.sources))
                    # sources[0] = DebugObservable(sources[0], 'd1')
                    left_selector = ObservableSelector(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                if all(isinstance(selector, IdentitySelector) for selector in right_selectors):
                    right_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(right_selectors, other.sources))
                    right_selector = ObservableSelector(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                return SelectorFound(
                    left=left_selector,
                    right=right_selector,
                )
            else:
                return NoSelectorFound()
        else:
            return NoSelectorFound()