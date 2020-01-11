from functools import reduce
from typing import Generic, Callable

import rx
from rx import operators as rxop
from rx.subject import Subject

import rxbp
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.multicasts.debugmulticast import DebugMultiCast
from rxbp.multicast.multicasts.defermulticast import DeferMultiCast
from rxbp.multicast.multicasts.filtermulticast import FilterMultiCast
from rxbp.multicast.multicasts.flatmapmulticast import FlatMapMultiCast
from rxbp.multicast.multicasts.liftmulticast import LiftMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.mergemulticast import MergeMultiCast
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicasts.zipmulticast import ZipMultiCast
from rxbp.multicast.rxextensions.merge_ import merge_op
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCast(MultiCastOpMixin, MultiCastBase, Generic[MultiCastValue]):
    """ A `MultiCast` represents a collection of *Flowable* and can
     be though of as `Flowable[T[Flowable]]` where T is defined by the user.
    """

    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    @classmethod
    def _copy(cls, multi_cast: MultiCastBase):
        return cls(multi_cast)

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.underlying.get_source(info=info)

    def connect_flowable(self, *others: 'MultiCast'):
        return self._copy(ZipMultiCast(sources=[self] + list(others)))

    def debug(
            self,
            name: str = None,
    ):

        return self._copy(DebugMultiCast(source=self, name=name))

    def loop_flowable(
            self,
            func: Callable[[MultiCastValue], MultiCastValue], initial: ValueType,
    ):
        def lifted_func(multicast: MultiCastBase):
            return func(MultiCast(multicast))

        return self._copy(DeferMultiCast(source=self, func=lifted_func, initial=initial))

    def empty(self):
        return rxbp.multicast.empty()

    def filter(
            self,
            predicate: Callable[[MultiCastValue], bool],
    ):
        return self._copy(FilterMultiCast(source=self, predicate=predicate))

    def flat_map(
            self,
            func: Callable[[MultiCastValue], 'MultiCast[MultiCastValue]'],
    ):
        return self._copy(FlatMapMultiCast(source=self, func=func))

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

    def map(
            self,
            func: Callable[[MultiCastValue], MultiCastValue],
    ):
        return self._copy(MapMultiCast(source=self, func=func))

    def merge(
            self,
            *others: 'MultiCast',
    ):
        sources = reversed([self] + list(others))

        return self._copy(MergeMultiCast(sources=sources))

    def pipe(self, *operators: MultiCastOperator) -> 'MultiCast':
        return reduce(lambda acc, op: op(acc), operators, self)

    def reduce_flowable(
            self,
            maintain_order: bool = None,
    ):
        return self._copy(ReduceMultiCast(source=self, maintain_order=maintain_order))

    def _share(self):
        subject = Subject()

        multicast = self._copy(SharedMultiCast(source=self, subject=subject))
        return multicast
