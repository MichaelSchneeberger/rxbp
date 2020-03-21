from functools import reduce
from typing import Generic, Callable, Any

import rx
from rx import operators as rxop
from rx.subject import Subject

import rxbp
from rxbp.multicast.flowableop import FlowableOp
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.multicasts.collectmulticast import CollectMultiCast
from rxbp.multicast.multicasts.debugmulticast import DebugMultiCast
from rxbp.multicast.multicasts.defaultifemptymulticast import DefaultIfEmptyMultiCast
from rxbp.multicast.multicasts.filtermulticast import FilterMultiCast
from rxbp.multicast.multicasts.firstmulticast import FirstMultiCast
from rxbp.multicast.multicasts.firstordefaultmulticast import FirstOrDefaultMultiCast
from rxbp.multicast.multicasts.flatmapmulticast import FlatMapMultiCast
from rxbp.multicast.multicasts.liftmulticast import LiftMultiCast
from rxbp.multicast.multicasts.loopflowablemulticast import LoopFlowableMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.mergemulticast import MergeMultiCast
from rxbp.multicast.multicasts.observeonmulticast import ObserveOnMultiCast
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.rxextensions.merge_ import merge_op
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCast(MultiCastOpMixin, MultiCastBase, Generic[MultiCastValue]):
    """
    A `MultiCast` represents a collection of *Flowable* and can
     be though of as `Flowable[T[Flowable]]` where T is defined by the user.
    """

    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    @classmethod
    def _copy(cls, multi_cast: MultiCastBase):
        return cls(multi_cast)

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.underlying.get_source(info=info)

    def join_flowables(self, *others: 'MultiCast'):
        if len(others) == 0:
            return self

        return self._copy(CollectMultiCast(sources=[self] + list(others)))

    def debug(
            self,
            name: str = None,
    ):

        return self._copy(DebugMultiCast(source=self, name=name))

    def default_if_empty(
            self,
            lazy_val: Callable[[], Any],
    ):
        return self._copy(DefaultIfEmptyMultiCast(source=self, lazy_val=lazy_val))

    def empty(self):
        return rxbp.multicast.empty()

    def filter(
            self,
            predicate: Callable[[MultiCastValue], bool],
    ):
        return self._copy(FilterMultiCast(source=self, predicate=predicate))

    def first(
            self,
            raise_exception: Callable[[Callable[[], None]], None],
    ):
        return self._copy(FirstMultiCast(source=self, raise_exception=raise_exception))

    def first_or_default(
            self,
            lazy_val: Callable[[], Any],
    ):
        return self._copy(FirstOrDefaultMultiCast(source=self, lazy_val=lazy_val))

    def flat_map(
            self,
            func: Callable[[MultiCastValue], 'MultiCast[MultiCastValue]'],
    ):
        return self._copy(FlatMapMultiCast(source=self, func=func))

    # def build_imperative_multicast(
    #         self,
    #         func: Callable[[Any], ImperativeCall]
    # ):
    #     return self._copy(FlatMapMultiCast(source=self, func=func))

    def lift(
            self,
            func: Callable[['MultiCast', MultiCastValue], MultiCastValue],
    ):
        def lifted_func(base: MultiCastBase, first: MultiCastValue):
            return func(MultiCast(base), first)

        return self._copy(LiftMultiCast(
            source=self,
            func=lifted_func,
        ))

    def loop(
            self,
            func: Callable[['MultiCast'], 'MultiCast[MultiCast]'],
    ):
        """        merge   flat_map    share
                ---->o------->o-------->o---------->
                     ^                  |
                     *------------------*
        """

        class LoopMultiCast(MultiCastBase):
            def get_source(
                    self,
                    info: MultiCastInfo,
            ) -> rx.typing.Observable[MultiCastValue]:
                source = self.get_source(info=info)

                shared_multi_cast = rx.defer(lambda: shared_multi_cast).pipe(
                    merge_op(source),
                    rxop.flat_map(lambda mc: func(mc).get_source(info=info)),
                    rxop.share(),
                )

                return shared_multi_cast

        return LoopMultiCast()

    def loop_flowables(
            self,
            func: Callable[[MultiCastValue], MultiCastValue], initial: ValueType,
    ):
        def lifted_func(multicast: MultiCastBase):
            return func(MultiCast(multicast))

        return self._copy(LoopFlowableMultiCast(source=self, func=lifted_func, initial=initial))

    def map(
            self,
            func: Callable[[MultiCastValue], MultiCastValue],
    ):
        return self._copy(MapMultiCast(source=self, func=func))

    def map_with_op(
            self,
            func: Callable[[MultiCastValue, FlowableOp], MultiCastValue],
    ):
        def inner_func(value: MultiCastValue):
            return func(value, FlowableOp())

        return self.map(inner_func)

    # def map_to_iterator(self, func: Callable[[MultiCastValue], Iterator[MultiCastValue]]):
    #     return self._copy(MapToIteratorMultiCast(source=self, func=func))

    def merge(
            self,
            *others: 'MultiCast',
    ):
        sources = reversed([self] + list(others))

        return self._copy(MergeMultiCast(sources=sources))

    def pipe(self, *operators: MultiCastOperator) -> 'MultiCast':
        return reduce(lambda acc, op: op(acc), operators, self)

    def observe_on(self, scheduler: rx.typing.Scheduler):
        return self._copy(ObserveOnMultiCast(source=self, scheduler=scheduler))

    def collect_flowables(
            self,
            maintain_order: bool = None,
    ):
        return self._copy(ReduceMultiCast(source=self, maintain_order=maintain_order))

    def _share(self):
        subject = Subject()

        multicast = self._copy(SharedMultiCast(source=self, subject=subject))
        return multicast
