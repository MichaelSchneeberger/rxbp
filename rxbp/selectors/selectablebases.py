# from abc import ABC, abstractmethod
# from typing import Any, Iterable, List
#
# from rxbp.observable import Observable
# from rxbp.observables.concatobservable import ConcatObservable
# from rxbp.observables.mergeobservable import MergeObservable
# from rxbp.scheduler import Scheduler
# from rxbp.selectors.bases import NumericalBase, ObjectRefBase, Base
# from rxbp.selectors.getselectormixin import IdentitySelector, SelectorResult, SelectorFound, NoSelectorFound, \
#     GetSelectorMixin, ObservableSelector, Selector
# from rxbp.selectors.observables.identityselectorobservable import IdentitySelectorObservable
# from rxbp.subscriber import Subscriber
# from rxbp.testing.debugobservable import DebugObservable
#
#
# class SelectableBase(GetSelectorMixin, ABC):
#     pass
#
#
# class NumericalSelectableBase(SelectableBase):
#     def __init__(self, base: NumericalBase, observable: Observable):
#         self.base = base
#         # self.observable = observable
#
#     def get_selectors(self, other: SelectableBase, subscriber: Subscriber) -> SelectorResult:
#         if isinstance(other, NumericalSelectableBase) and self.base.num == other.base.num:
#             return SelectorFound(IdentitySelector(), IdentitySelector())
#         else:
#             return NoSelectorFound()
#
#
# class ObjectRefSelectableBase(SelectableBase):
#     def __init__(self, base: ObjectRefBase, observable: Observable):
#         self.base = base
#         # self.observable = observable
#
#     def get_selectors(self, other: SelectableBase, subscriber: Subscriber) -> SelectorResult:
#         if isinstance(other, ObjectRefSelectableBase) and self.base.obj == other.base.obj:
#             return SelectorFound(IdentitySelector(), IdentitySelector())
#         else:
#             return NoSelectorFound()
#
# class PairwiseSelectableBase(SelectableBase):
#     def __init__(self, underlying: SelectableBase):
#         self.underlying = underlying
#
#     def get_selectors(self, other: SelectableBase, subscriber: Subscriber) -> SelectorResult:
#         if isinstance(other, PairwiseSelectableBase):
#             return self.underlying.get_selectors(other.underlying, subscriber=subscriber)
#         else:
#             return NoSelectorFound()
#
#
# def from_base(base: Base, ):
#     if isinstance(base, NumericalBase):
#         return NumericalSelectableBase(base=base, observable=observable)
#     elif isinstance(base, ObjectRefBase):
#         return ObjectRefSelectableBase(base=base, observable=observable)
#     else:
#         raise Exception('no selectable base found for base "{}"'.format(base))
#
#
# class ConcatSelectableBase(SelectableBase):
#     def __init__(
#             self,
#             underlying: Iterable[GetSelectorMixin],
#             sources: List[Observable],
#     ):
#         self.underlying = list(underlying)
#         self.sources = [IdentitySelectorObservable(source) for source in sources]
#
#     def get_selectors(self, other: Base, subscriber: Subscriber) -> SelectorResult:
#         if isinstance(other, ConcatSelectableBase):
#             other_base = other
#
#             def gen_selectors():
#                 for left, right in zip(self.underlying, other_base.underlying):
#                     # print(left.base)
#                     # print(right.base)
#                     result = left.get_selectors(right, subscriber)
#                     yield result
#             results = list(gen_selectors())
#
#             if all(isinstance(result, SelectorFound) for result in results):
#                 typed_results: List[SelectorFound] = results
#
#                 left_selectors, right_selectors = zip(*[(result.left, result.right) for result in typed_results])
#
#                 def gen_observables(selectors: List[Selector], sources: List[Observable]):
#                     for selector, source in zip(selectors, sources):
#                         if isinstance(selector, IdentitySelector):
#                             yield source
#                         elif isinstance(selector, ObservableSelector):
#                             yield selector.observable
#
#                 # print(left_selectors)
#                 # print(right_selectors)
#
#                 if all(isinstance(selector, IdentitySelector) for selector in left_selectors):
#                     left_selector = IdentitySelector()
#                 else:
#                     sources = list(gen_observables(left_selectors, self.sources))
#                     # sources[0] = DebugObservable(sources[0], 'd1')
#                     left_selector = ObservableSelector(ConcatObservable(
#                         sources=sources,
#                         scheduler=subscriber.scheduler,
#                         subscribe_scheduler=subscriber.subscribe_scheduler,
#                     ))
#
#                 if all(isinstance(selector, IdentitySelector) for selector in right_selectors):
#                     right_selector = IdentitySelector()
#                 else:
#                     sources = list(gen_observables(right_selectors, other.sources))
#                     right_selector = ObservableSelector(ConcatObservable(
#                         sources=sources,
#                         scheduler=subscriber.scheduler,
#                         subscribe_scheduler=subscriber.subscribe_scheduler,
#                     ))
#
#                 return SelectorFound(
#                     left=left_selector,
#                     right=right_selector,
#                 )
#             else:
#                 return NoSelectorFound()
#         else:
#             return NoSelectorFound()
