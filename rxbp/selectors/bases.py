from abc import ABC, abstractmethod
from typing import Any, Iterable, List

from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.getselectormixin import IdentitySelector, SelectorResult, SelectorFound, NoSelectorFound, \
    GetSelectorMixin, ObservableSelector, Selector
from rxbp.selectors.observables.identityselectorobservable import IdentitySelectorObservable
from rxbp.subscriber import Subscriber


class Base(GetSelectorMixin, ABC):
    @abstractmethod
    def get_name(self):
        ...


class NumericalBase(Base):
    def __init__(self, num: int):
        self.num = num

    def get_name(self):
        return '{}({})'.format(self.__class__.__name__, self.num)

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, NumericalBase) and self.num == other.num:
            return SelectorFound(IdentitySelector(), IdentitySelector())
        else:
            return NoSelectorFound()


class ObjectRefBase(Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def get_name(self):
        return '{}({})'.format(self.__class__.__name__, self.obj)

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return SelectorFound(IdentitySelector(), IdentitySelector())
        else:
            return NoSelectorFound()


class PairwiseBase(Base):
    def __init__(self, underlying: Base):
        self.underlying = underlying

    def get_name(self):
        return 'PairwiseBase({})'.format(self.underlying.get_name())

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

    def get_name(self):
        def gen_names():
            for info in self.underlying:
                base = info.base
                if isinstance(base, Base):
                    yield base.get_name()
                else:
                    yield str(base)

        return 'ConcatBase({})'.format(', '.join(list(gen_names())))

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    # if isinstance(left.base, NumericalBase) and isinstance(right.base, NumericalBase):
                    #     if left.base.num == 0 and right.base.num == 0:
                    #         continue
                    result = left.get_selectors(right, subscriber)
                    yield result
            results = list(gen_selectors())

            if all(isinstance(result, SelectorFound) for result in results):
                typed_results: List[SelectorFound] = results

                left_selectors, right_selectors = zip(*[(result.left, result.right) for result in typed_results])

                def gen_observables(selectors: List[Selector], sources: List[Observable]):
                    assert len(selectors) == len(sources)

                    for selector, source in zip(selectors, sources):
                        if isinstance(selector, IdentitySelector):
                            yield source
                        elif isinstance(selector, ObservableSelector):
                            yield selector.observable

                if all(isinstance(selector, IdentitySelector) for selector in left_selectors):
                    left_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(left_selectors, self.sources))
                    left_selector = ObservableSelector(ConcatObservable(
                        sources=sources,
                        scheduler=subscriber.scheduler,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                    ))

                if all(isinstance(selector, IdentitySelector) for selector in right_selectors):
                    right_selector = IdentitySelector()
                else:
                    sources = list(gen_observables(right_selectors, other.sources))
                    # print(sources)
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