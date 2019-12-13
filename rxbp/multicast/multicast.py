from functools import reduce
from typing import Generic, Callable, Union, List, Dict

import rx
import rxbp
from rx import operators as rxop
from rx.subject import Subject
from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.multicasts.defermulticast import DeferMultiCast
from rxbp.multicast.multicasts.extendmulticast import ExtendMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.multicasts.zipmulticast import ZipMultiCast
from rxbp.multicast.rxextensions.debug_ import debug as rx_debug
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCast(MultiCastOpMixin, MultiCastBase, Generic[MultiCastValue]):
    """ A `MultiCast` represents a collection of *Flowable* and can
     be though of as `Flowable[T[Flowable]]` where T is defined by the user.
    """

    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    def pipe(self, *operators: MultiCastOperator) -> 'MultiCast':
        return reduce(lambda acc, op: op(acc), operators, self)

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.underlying.get_source(info=info)

    def debug(self, name: str):
        class DebugMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                print(f'{name}.get_source({info})')

                return self.get_source(info=info).pipe(
                    rx_debug(name),
                )

        return MultiCast(DebugMultiCast())

    def defer(self, func: Callable[[MultiCastValue], MultiCastValue], initial: ValueType):
        def lifted_func(multicast: MultiCastBase):
            return func(MultiCast(multicast))

        return MultiCast(DeferMultiCast(source=self, func=lifted_func, initial=initial))

    def empty(self):
        return rxbp.multicast.empty()

    def extend(self, func: Callable[[MultiCastValue], Union[Flowable, List, Dict, FlowableStateMixin]]):
        return MultiCast(ExtendMultiCast(source=self, func=func))

    def filter(self, func: Callable[[MultiCastValue], bool]):
        class FilterMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                source = self.get_source(info=info).pipe(
                    rxop.filter(func)
                )
                return source

        return MultiCast(FilterMultiCast())

    def flat_map(self, func: Callable[[MultiCastValue], 'MultiCast[MultiCastValue]']):
        class FlatMapMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                return self.get_source(info=info).pipe(
                    rxop.flat_map(lambda v: func(v).get_source(info=info)),
                )

        return MultiCast(FlatMapMultiCast())

    def lift(self, func: Callable[['MultiCast'], MultiCastValue]):
        class LiftMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                class InnerLiftMultiCast(MultiCastBase):
                    def __init__(self, source: Flowable[MultiCastValue]):
                        self._source = source

                    def get_source(self, info: MultiCastInfo) -> Flowable:
                        return self._source

                source = self.get_source(info=info).pipe(
                    rxop.share(),
                )

                inner_multicast = InnerLiftMultiCast(source=source)
                multicast_val = func(MultiCast(inner_multicast))
                return rx.return_value(multicast_val, scheduler=info.multicast_scheduler)
        return MultiCast(LiftMultiCast())

    def merge(self, *others: 'MultiCast'):
        class MergeMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                multicasts = reversed([self] + list(others))
                return rx.merge(*[e.get_source(info=info) for e in multicasts])

        return MultiCast(MergeMultiCast())

    def map(self, func: Callable[[MultiCastValue], MultiCastValue]):
        return MultiCast(MapMultiCast(source=self, func=func))

    def reduce(self):
        return MultiCast(ReduceMultiCast(source=self))

    def share(self):
        subject = Subject()

        class SharedMultiCast(MultiCastBase):
            def get_source(_, info: MultiCastInfo) -> rx.typing.Observable:
                shared_source = self.get_source(info=info).pipe(
                    rxop.multicast(subject=subject),
                    rxop.ref_count(),
                )

                return shared_source

        multicast = MultiCast(SharedMultiCast())
        return multicast

    def zip(self, *others: 'MultiCast'):
        return MultiCast(ZipMultiCast(sources=[self] + list(others)))
