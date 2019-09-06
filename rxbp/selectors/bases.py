from abc import ABC, abstractmethod
from typing import Any, Iterable, List

from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.getselectormixin import IdentitySelector, SelectorResult, SelectorFound, NoSelectorFound, \
    GetSelectorMixin, ObservableSelector
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
    ):
        self.underlying = list(underlying)

    def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
        if isinstance(other, ConcatBase):
            other_base = other

            def gen_selectors():
                for left, right in zip(self.underlying, other_base.underlying):
                    result = left.get_selectors(right, subscriber)
                    yield result
            results = list(gen_selectors())

            if all(isinstance(result, SelectorFound) for result in results):
                typed_results: List[SelectorFound] = results

                left, right = zip(*[(result.left, result.right) for result in typed_results])
                return SelectorFound(
                    left=ObservableSelector(ConcatObservable(sources=left, subscribe_scheduler=subscriber.subscribe_scheduler)),
                    right=ObservableSelector(ConcatObservable(sources=left, subscribe_scheduler=subscriber.subscribe_scheduler))
                )
            else:
                return NoSelectorFound()
        else:
            return NoSelectorFound()


# class SourceBaseMixin(ABC):
#     @abstractmethod
#     def equals(self, other: 'Base') -> bool:
#         ...
#
#
# class Base(ABC):
#
#     def get_selection_tree(self, other: 'Base') -> Tree:
#         """
#
#         b1 = ConcatBase(s1, s2, s3)
#         b2 = ConcatBase(s1, s2' {s2 -> s2'}, s3)
#
#         None = b1.get_selection_tree(b2)
#         tree = b2.get_selection_tree(b1)
#
#         tree = UseSelector(
#             children = [
#                 LeaveTree(ident),
#                 UseSelector(
#                     children = [
#                         LeaveTree(ident),
#                     ],
#                     selector_basse = s2,
#                 )
#             ],
#             selector_base = ident,
#         )
#
#         """
#
#         pass
#
#     # @abstractmethod
#     def is_matching(self, other: 'Base') -> bool:
#         """ two bases match if the source base they are derived from is the same
#         """
#
#         # sources1 = self #.get_source_bases()
#         # sources2 = other #.get_source_bases()
#
#         # if len(sources1) != len(sources2):
#         #     return False
#
#         # def gen_source_comparison():
#         #     for s1, s2 in zip(sources1, sources2):
#         #
#         #         assert isinstance(s1, SourceBaseMixin) and isinstance(s2, SourceBaseMixin), \
#         #             'first element in base sequence must be a Source'
#         #
#         #         yield s1.equals(s2)
#         #
#         # return all(gen_source_comparison())
#
#         assert isinstance(self, SourceBaseMixin) and isinstance(other, SourceBaseMixin), \
#                         'Bases "{}" and "{}" must be sources'.format(self, other)
#
#         return self.equals(other)
#
#     @abstractmethod
#     def equals(self, other: 'Base') -> bool:
#         ...
#
#     @abstractmethod
#     def get_source_bases(self) -> List['Base']:    # todo: really neede?
#         ...
#
#
# class SourceBase(SourceBaseMixin, Base, ABC):
#     pass
#
#
# class NumericalBase(SourceBase):
#     def __init__(self, num: int):
#         self.num = num
#
#     def equals(self, other: FlowableBase):
#         if isinstance(other, NumericalBase) and self.num == other.num:
#             return True
#         else:
#             return False
#
#     def get_source_bases(self) -> List['Base']:
#         return [self]
#
#
# class ObjectRefBase(SourceBase):
#     def __init__(self, obj: Any = None):
#         self.obj = obj or self
#
#     def equals(self, other: FlowableBase):
#         if isinstance(other, ObjectRefBase) and self.obj == other.obj:
#             return True
#         else:
#             return False
#
#     def get_source_bases(self) -> List['Base']:
#         return [self]
#
#
# class SharedBase(SourceBase):
#     def __init__(self, prev_base: FlowableBase = None):
#         # self.has_fan_out = has_fan_out
#         self._prev_base = prev_base or ObjectRefBase(self)
#
#     def get_source_bases(self) -> List['Base']:
#         return self._prev_base.get_source_bases()
#
#
# class PairwiseBase(SourceBase):
#     def __init__(self, underlying: FlowableBase):
#         self.underlying = underlying
#
#     def equals(self, other: FlowableBase):
#         if isinstance(other, PairwiseBase):
#             self_source = self.underlying #.get_source_bases()[0]
#             other_source = other.underlying #.get_source_bases()[0]
#
#             assert isinstance(self_source, SourceBaseMixin) and isinstance(other_source, SourceBaseMixin), \
#                 'first element in base sequence must be a Source'
#
#             return self_source.equals(other_source)
#         else:
#             return False
#
#     def get_source_bases(self) -> List[Base]:
#         return [self]
#
#
# class ConcatBase(SourceBase):
#     def __init__(self, underlying: Iterable[Base]):
#         self.underlying = list(underlying)
#
#     def equals(self, other: FlowableBase):
#         if isinstance(other, ConcatBase):
#             concat_base = other
#             print('ok')
#
#             def gen_equals_results():
#                 for s1, s2 in zip(self.underlying, concat_base.underlying):
#                     assert isinstance(s1, SourceBaseMixin) and isinstance(s2, SourceBaseMixin), \
#                         'first element in base sequence must be a Source'
#
#                     print(s1)
#                     print(s2)
#                     yield s1.equals(s2)
#
#             return all(gen_equals_results())
#         else:
#             return False
#
#     def get_source_bases(self) -> List[Base]:
#         return [source for base in self.underlying for source in base.get_source_bases()]
