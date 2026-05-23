from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, override

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.asyncshare.flowable import (
    init_async_share_flowable_node,
)
from rxbp.flowabletree.operations.asyncshare.subject import AsyncShareSubject
from rxbp.flowabletree.operations.map import init_map_flowable
from rxbp.flowabletree.operations.share.flowable import init_share_flowable_node
from rxbp.flowabletree.operations.tap import init_tap_flowable
from rxbp.flowabletree.state import State
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult


class Flowable[S: State, A: SubscribeArgs, U, E](
    SingleChildFlowableNode[S, A, U, U, E], ABC
):
    # def accumulate(self, func, init):
    #     return self.copy(child=init_accumulate_flowable(self.p_child, func, init))
    #
    # def batch(self, size: int):
    #     return self.copy(child=init_to_list_flowable(child=self.p_child, size=size))
    #
    # def buffer(self):
    #     return self.copy(
    #         child=init_buffer(
    #             child=self.p_child,
    #         )
    #     )

    @abstractmethod
    def copy[V, E2](
        self, child: FlowableNode[S, A, V, E2]
    ) -> Flowable[S, A, V, E2]: ...

    # def concat_map(self, func: Callable[[U], FlowableNode]):
    #     return self.copy(
    #         child=init_concat_map(
    #             child=self.p_child,
    #             func=func,
    #         )
    #     )
    #
    # def default_if_empty[V](self, value: V):
    #     return self.copy(
    #         child=init_default_if_empty_flowable(
    #             child=self.p_child,
    #             value=value,
    #         )
    #     )

    # def filter(self, predicate: Callable[[U], bool]) -> Flowable[S, U]:
    #     return self.copy(
    #         child=init_filter_flowable(
    #             child=self.p_child,
    #             predicate=predicate,
    #         )
    #     )

    # def first(self):
    #     return self.take(count=1)
    #
    # def flat_map(self, func: Callable[[U], FlowableNode]):
    #     return self.copy(
    #         child=init_flat_map_node(
    #             child=self.p_child,
    #             func=func,
    #             stack=get_frame_summary(),
    #         )
    #     )
    #
    # # def flatten(self):
    # #     return self.flat_map(lambda f: f)
    #
    # def last(self):
    #     return self.to_list(size=1).map(lambda v: v[0])
    #     # return self.copy(child=init_last_flowable(self.child))

    def map[V](self, func: Callable[[U], V]) -> Flowable[S, A, V, E | Exception]:
        return self.copy(
            child=init_map_flowable(
                child=self.p_child,
                func=func,
                stacks=tuple(),
            ),
        )

    # def reduce(self, func):
    #     return self.copy(child=init_reduce_flowable(self.p_child, func))
    #
    # def repeat(self, count: int | None = None):
    #     return (
    #         self.copy(child=_repeat_value(self))
    #         .take(count)
    #         .flatten()
    #     )
    #
    # def repeat_first(self):
    #     return self.first().flat_map(lambda v: _repeat_value(v))
    #
    # @abstractmethod
    # def seq(self):
    #     ...

    def share(self):
        return self.copy(
            child=init_share_flowable_node(
                child=self.p_child,
            )
        )

    def hot_share(self, subject: AsyncShareSubject[S, A, U, E]):
        return self.copy(
            child=init_async_share_flowable_node(
                child=self.p_child,
                subject=subject,
            )
        )

    # def skip(self, count: int):
    #     return (
    #         self.copy(child=init_zip_flowable_node((self, _count())))
    #         .skip_while(lambda v: v[1] < count)
    #         .map(lambda v: v[0])
    #     )
    #
    # def skip_while(self, predicate):
    #     return self.copy(
    #         child=init_skip_while_flowable(child=self.p_child, predicate=predicate)
    #     )
    #
    # def take(self, count: int | None = None):
    #     if count is None:
    #         return self
    #
    #     return (
    #         self.zip_with_index().take_while(lambda v: v[1] < count).map(lambda v: v[0])
    #     )
    #
    # def take_while(self, predicate):
    #     return self.copy(
    #         child=init_take_while_flowable(child=self.p_child, predicate=predicate)
    #     )

    def tap(
        self,
        on_next: Callable[[U], None] | None = None,
        on_next_and_complete: Callable[[U], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        on_error: Callable[[E], None] | None = None,
    ):
        return self.copy(
            child=init_tap_flowable(
                self.p_child,
                on_next,
                on_next_and_complete,
                on_completed,
                on_error,
            )
        )

    # def to_list(self):
    #     return self.copy(child=init_to_list_flowable(child=self.p_child))
    #
    # def zip_with_index(self):
    #     return self.copy(child=init_zip_flowable_node((self, _count())))
    #
    # def zip(self, others: tuple[Flowable, ...]):
    #     return self.copy(
    #         child=init_zip_flowable_node(children=(self,) + others),
    #     )

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        return self.p_child.unsafe_subscribe(state, observer, args)

    # @property
    # @override
    # def p_child(self) -> FlowableNode[S, U]:
    #     return self.child


@dataclass(frozen=True)
class FlowableImpl[S: State, A: SubscribeArgs, U, E](Flowable[S, A, U, E]):
    child: FlowableNode[S, A, U, E]

    @override
    def copy[V](self, child: FlowableNode[S, A, V, E]) -> FlowableImpl[S, A, V, E]:
        return init_flowable(child=child)

    @property
    @override
    def p_child(self) -> FlowableNode[S, A, U, E]:
        return self.child


def init_flowable[S: State, A: SubscribeArgs, U, E](child: FlowableNode[S, A, U, E]):
    return FlowableImpl(child=child)


# class ConnectableFlowable[U](Flowable[U]): ...
