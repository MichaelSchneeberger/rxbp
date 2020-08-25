from abc import ABC, abstractmethod
from typing import Callable, Any

import rx

import rxbp
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.collectflowablesmulticast import CollectFlowablesMultiCast
from rxbp.multicast.multicasts.defaultifemptymulticast import DefaultIfEmptyMultiCast
from rxbp.multicast.multicasts.filtermulticast import FilterMultiCast
from rxbp.multicast.multicasts.firstmulticast import FirstMultiCast
from rxbp.multicast.multicasts.firstordefaultmulticast import FirstOrDefaultMultiCast
from rxbp.multicast.multicasts.flatmapmulticast import FlatMapMultiCast
from rxbp.multicast.multicasts.init.initdebugmulticast import init_debug_multi_cast
from rxbp.multicast.multicasts.joinflowablesmulticast import JoinFlowablesMultiCast
from rxbp.multicast.multicasts.loopflowablemulticast import LoopFlowableMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.mergemulticast import MergeMultiCast
from rxbp.multicast.multicasts.observeonmulticast import ObserveOnMultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem
from rxbp.typing import ValueType


class MultiCastOpMixin(MultiCastMixin, ABC):
    @property
    @abstractmethod
    def underlying(self) -> MultiCastMixin:
        ...

    @abstractmethod
    def _copy(self, *args, **kwargs) -> 'MultiCastOpMixin':
        ...

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    def collect_flowables(
            self,
            maintain_order: bool = None,
    ):
        return self._copy(CollectFlowablesMultiCast(source=self, maintain_order=maintain_order))

    def debug(
            self,
            name: str = None,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_subscribe: Callable[[MultiCastObserverInfo, MultiCastSubscriber], None] = None,
            verbose: bool = None,
    ):

        return self._copy(init_debug_multi_cast(
            source=self,
            name=name,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_subscribe=on_subscribe,
            verbose=verbose,
        ))

    def default_if_empty(
            self,
            lazy_val: Callable[[], Any],
    ):
        return self._copy(DefaultIfEmptyMultiCast(source=self, lazy_val=lazy_val))

    def empty(self):
        return rxbp.multicast.empty()

    def filter(
            self,
            predicate: Callable[[MultiCastItem], bool],
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
            func: Callable[[MultiCastItem], 'MultiCast[MultiCastItem]'],
    ):
        return self._copy(FlatMapMultiCast(source=self, func=func))

    def join_flowables(self, *others: 'MultiCastMixin'):
        if len(others) == 0:
            return self

        return self._copy(JoinFlowablesMultiCast(sources=[self] + list(others)))

    def share_func(
            self,
            func: Callable[['MultiCastOpMixin'], 'MultiCastOpMixin'],
    ):
        return self._copy(
            underlying=func(self._copy(
                underlying=SharedMultiCast(source=self),
            )),
        )

    # def build_imperative_multicast(
    #         self,
    #         func: Callable[[Any], ImperativeCall]
    # ):
    #     return self._copy(FlatMapMultiCast(source=self, func=func))

    # def loop(
    #         self,
    #         func: Callable[['MultiCastMixin'], 'MultiCast[MultiCast]'],
    # ):
    #     """        merge   flat_map    share
    #             ---->o------->o-------->o---------->
    #                  ^                  |
    #                  *------------------*
    #     """
    #
    #     class LoopMultiCast(MultiCastMixin):
    #         def unsafe_subscribe(
    #                 self,
    #                 info: MultiCastInfo,
    #         ) -> rx.typing.Observable[MultiCastItem]:
    #             source = self.get_source(info=info)
    #
    #             shared_multi_cast = rx.defer(lambda: shared_multi_cast).pipe(
    #                 merge_op(source),
    #                 rxop.flat_map(lambda mc: func(mc).get_source(info=info)),
    #                 rxop.share(),
    #             )
    #
    #             return shared_multi_cast
    #
    #     return LoopMultiCast()

    def lift(
            self,
            func: Callable[['MultiCast'], Any],
    ):
        raise Exception('Only non-lifted MultiCast implements the lift operator')


    def loop_flowables(
            self,
            func: Callable[[MultiCastItem], MultiCastItem],
            initial: ValueType,
    ):
        # def lifted_func(multicast: MultiCastMixin):
        #     # return func(init_multicast(multicast))
        #     return multicast

        return self._copy(LoopFlowableMultiCast(source=self, func=func, initial=initial))

    def map(
            self,
            func: Callable[[MultiCastItem], MultiCastItem],
    ):
        return self._copy(MapMultiCast(source=self, func=func))

    # def map_to_iterator(self, func: Callable[[MultiCastValue], Iterator[MultiCastValue]]):
    #     return self._copy(MapToIteratorMultiCast(source=self, func=func))

    def merge(
            self,
            *others: 'MultiCastMixin',
    ):
        sources = reversed([self] + list(others))

        return self._copy(MergeMultiCast(sources=sources))

    def observe_on(self, scheduler: rx.typing.Scheduler):
        return self._copy(ObserveOnMultiCast(source=self, scheduler=scheduler))
