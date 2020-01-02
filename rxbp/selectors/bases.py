from dataclasses import dataclass
from typing import Any, Iterable, List

from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.base import Base
from rxbp.selectors.baseselectorstuple import BaseSelectorsTuple
from rxbp.selectors.selector import Selector, IdentitySelector, ObservableSelector
from rxbp.selectors.observables.identityselectorobservable import IdentitySelectorObservable
from rxbp.subscriber import Subscriber


class NumericalBase(Base):
    def __init__(self, num: int):
        self.num = num

    def get_name(self):
        return f'{self.__class__.__name__}({self.num})'

    def get_selectors(self, other: Base, subscriber: Subscriber):
        if isinstance(other, NumericalBase) and self.num == other.num:
            return Base.MatchedBaseMapping(
                left=IdentitySelector(),
                right=IdentitySelector(),
                base=self,
            )
        else:
            return None


class ObjectRefBase(Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def get_name(self):
        return f'{self.__class__.__name__}({self.obj})'

    def get_selectors(self, other: Base, subscriber: Subscriber):
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return Base.MatchedBaseMapping(
                left=IdentitySelector(),
                right=IdentitySelector(),
                base=self,
            )
        else:
            return None


class PairwiseBase(Base):
    def __init__(self, underlying: Base):
        self.underlying = underlying

    def get_name(self):
        return f'PairwiseBase({self.underlying.get_name()})'

    def get_selectors(self, other: Base, subscriber: Subscriber):
        if isinstance(other, PairwiseBase):
            result: Base.MatchedBaseMapping = self.underlying.get_selectors(other.underlying, subscriber=subscriber)

            # after pairing, one cannot be transformed into the other
            if isinstance(result, Base.MatchedBaseMapping):
                if isinstance(result.left, IdentitySelector) and isinstance(result.right, IdentitySelector):
                    return result

        return None


class ConcatBase(Base):
    def __init__(
            self,
            underlying: Iterable[BaseSelectorsTuple],
            sources: List[Observable],
    ):
        self.underlying = list(underlying)
        self.sources = [IdentitySelectorObservable(source) for source in sources]

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

    def get_selectors(self, other: Base, subscriber: Subscriber):
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    yield left.get_selectors(right, subscriber)
            selector_results = list(gen_selectors())

            if all(isinstance(result, MatchedBaseMap) for result in selector_results):
                typed_results: List[MatchedBaseMap] = selector_results

                left_selectors, right_selectors = zip(*[(result.left, result.right) for result in typed_results])

                def gen_observables(selectors: List[Selector], sources: List[Observable]):
                    assert len(selectors) == len(sources)

                    for selector, source in zip(selectors, sources):
                        if isinstance(selector, IdentitySelector):
                            yield source
                        elif isinstance(selector, ObservableSelector):
                            yield selector.observable

                base = None,
                selectors = {}

                if all(isinstance(selector, IdentitySelector) for selector in left_selectors):
                    base = self
                    left_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(left_selectors, self.sources))
                    left_selector = ObservableSelector(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                if all(isinstance(selector, IdentitySelector) for selector in right_selectors):
                    base = other
                    right_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(right_selectors, other.sources))
                    right_selector = ObservableSelector(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                return MatchedBaseMap(
                    left=left_selector,
                    right=right_selector,
                    base=base,
                    selectors=selectors
                )
            else:
                return CouldNotMatch()
        else:
            return CouldNotMatch()