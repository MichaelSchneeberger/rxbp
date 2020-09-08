from abc import ABC, abstractmethod
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

import rx

import rxbp
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.materializemulticastobservable import MaterializeMultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.assertsinglesubscriptionmulticast import AssertSingleSubscriptionMultiCast
from rxbp.multicast.multicasts.collectflowablesmulticast import CollectFlowablesMultiCast
from rxbp.multicast.multicasts.debugmulticast import DebugMultiCast
from rxbp.multicast.multicasts.defaultifemptymulticast import DefaultIfEmptyMultiCast
from rxbp.multicast.multicasts.filtermulticast import FilterMultiCast
from rxbp.multicast.multicasts.firstmulticast import FirstMultiCast
from rxbp.multicast.multicasts.firstordefaultmulticast import FirstOrDefaultMultiCast
from rxbp.multicast.multicasts.flatmapmulticast import FlatMapMultiCast
from rxbp.multicast.multicasts.joinflowablesmulticast import JoinFlowablesMultiCast
from rxbp.multicast.multicasts.loopflowablemulticast import LoopFlowableMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.mergemulticast import MergeMultiCast
from rxbp.multicast.multicasts.observeonmulticast import ObserveOnMultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType


class MultiCastOpMixin(MultiCastMixin, ABC):
    @property
    @abstractmethod
    def underlying(self) -> MultiCastMixin:
        ...

    @abstractmethod
    def _copy(self, **kwargs) -> 'MultiCastOpMixin':
        ...

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    def assert_single_subscription(
            self,
            stack: List[FrameSummary],
    ):
        return self._copy(underlying=AssertSingleSubscriptionMultiCast(
            source=self,
            stack=stack,
            is_first=True,
        ))

    def collect_flowables(
            self,
            stack: List[FrameSummary],
            maintain_order: bool = None,
    ):
        return self._copy(underlying=CollectFlowablesMultiCast(
            source=self,
            stack=stack,
            maintain_order=maintain_order,
        ))

    def debug(
            self,
            stack: List[FrameSummary],
            name: str = None,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_subscribe: Callable[[MultiCastSubscriber], None] = None,
            on_observe: Callable[[MultiCastObserverInfo], None] = None,
            on_dispose: Callable[[], None] = None,
            verbose: bool = None,
    ):

        if verbose is None:
            verbose = True

        if verbose:
            on_next_func = on_next or (lambda v: print(f'{name}.on_next {v}'))
            on_error_func = on_error or (lambda exc: print(f'{name}.on_error {exc}'))
            on_completed_func = on_completed or (lambda: print(f'{name}.on_completed'))
            on_subscribe_func = on_subscribe or (lambda v: print(f'{name}.on_subscribe {v}'))
            on_observe_func = on_observe or (lambda v: print(f'{name}.on_observe {v}'))
            on_dispose_func = on_dispose or (lambda: print(f'{name}.on_disposed'))

        else:
            def empty_func0():
                return None

            def empty_func1(v):
                return None

            on_next_func = on_next or empty_func1
            on_error_func = on_error or empty_func1
            on_completed_func = on_completed or empty_func0
            on_subscribe_func = on_subscribe or empty_func1
            on_observe_func = on_subscribe or empty_func1
            on_dispose_func = empty_func0()

        return self._copy(underlying=DebugMultiCast(
            source=self,
            name=name,
            on_next=on_next_func,
            on_completed=on_completed_func,
            on_error=on_error_func,
            on_subscribe=on_subscribe_func,
            on_observe=on_observe_func,
            on_dispose=on_dispose_func,
            stack=stack,
        ))

    def default_if_empty(
            self,
            lazy_val: Callable[[], Any],
    ):
        return self._copy(
            underlying=DefaultIfEmptyMultiCast(source=self, lazy_val=lazy_val),
        )

    def empty(self):
        return rxbp.multicast.empty()

    def filter(
            self,
            predicate: Callable[[MultiCastItem], bool],
    ):
        return self._copy(
            underlying=FilterMultiCast(source=self, predicate=predicate),
        )

    def first(
            self,
            # raise_exception: Callable[[Callable[[], None]], None],
            stack: List[FrameSummary],
    ):
        return self._copy(
            underlying=FirstMultiCast(source=self, stack=stack),
        )

    def first_or_default(
            self,
            lazy_val: Callable[[], Any],
    ):
        return self._copy(
            underlying=FirstOrDefaultMultiCast(source=self, lazy_val=lazy_val),
        )

    def flat_map(
            self,
            func: Callable[[MultiCastItem], 'MultiCast[MultiCastItem]'],
            stack: List[FrameSummary],
    ):
        return self._copy(underlying=FlatMapMultiCast(
            source=self,
            func=func,
            stack=stack,
        ))

    def join_flowables(self, others: List['MultiCastMixin'], stack: List[FrameSummary]):
        if len(others) == 0:
            return self.map(lambda v: [v])

        return self._copy(
            underlying=JoinFlowablesMultiCast(sources=[self] + others, stack=stack),
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
            # func: Callable[['MultiCast'], Any],
    ):
        raise Exception('Only non-lifted MultiCast implements the lift operator')


    def loop_flowables(
            self,
            func: Callable[[MultiCastItem], MultiCastItem],
            initial: ValueType,
            stack: List[FrameSummary],
    ):
        return self._copy(underlying=LoopFlowableMultiCast(
            source=self,
            func=func,
            initial=initial,
            stack=stack,
        ))

    def materialize(
            self,
    ):
        @dataclass
        class MaterializeMultiCast(MultiCastMixin):
            source: MultiCastMixin

            def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
                subscription = self.source.unsafe_subscribe(subscriber=subscriber)
                return subscription.copy(
                    observable=MaterializeMultiCastObservable(
                        source=subscription.observable,
                    )
                )

        return self._copy(underlying=MaterializeMultiCast(source=self))

    def map(
            self,
            func: Callable[[MultiCastItem], MultiCastItem],
    ):
        return self._copy(underlying=MapMultiCast(source=self, func=func))

    # def map_to_iterator(self, func: Callable[[MultiCastValue], Iterator[MultiCastValue]]):
    #     return self._copy(MapToIteratorMultiCast(source=self, func=func))

    def merge(
            self,
            *others: 'MultiCastMixin',
    ):
        sources = list(reversed([self] + list(others)))

        lift_index = max(m.lift_index for m in sources)

        return self._copy(
            underlying=MergeMultiCast(sources=sources),
            lift_index=lift_index,
        )

    def observe_on(self, scheduler: Scheduler = None):
        return self._copy(underlying=ObserveOnMultiCast(source=self, scheduler=scheduler))

    def share_func(
            self,
            func: Callable[['MultiCastOpMixin'], 'MultiCastOpMixin'],
            stack: List[FrameSummary],
    ):
        underlying = func(self._copy(
            underlying=self._share(stack=stack),
        ))
        return underlying

    def _share(self, stack: List[FrameSummary]):
        return self._copy(
            underlying=SharedMultiCast(source=self, stack=stack),
        )
