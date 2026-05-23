from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self, override


from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.asyncshare.cancellable import AsyncShareCancellable
from rxbp.flowabletree.operations.asyncshare.requestobserver import (
    AsyncShareRequestObserver,
)
from rxbp.flowabletree.operations.asyncshare.states import ActiveState, ErrorState
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    SubscribeTransition,
)
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode
from rxbp.flowabletree.operations.asyncshare.observer import AsyncShareObserver
from rxbp.flowabletree.state import State


class AsyncShareSubscribeArgs(SubscribeArgs, ABC):
    @property
    @abstractmethod
    def p_async_share_observers(
        self,
    ) -> dict[AsyncShareSubject[Any, Any, Any, Any], AsyncShareObserver[Any, Any]]:  # pyright: ignore[reportExplicitAny]
        ...

    @abstractmethod
    def update_async_share_observers(
        self,
        observers: dict[
            AsyncShareSubject[Any, Any, Any, Any], AsyncShareObserver[Any, Any]  # pyright: ignore[reportExplicitAny]
        ],
    ) -> Self: ...


@dataclass(frozen=True)
class AsyncShareSubject[S: State, A: AsyncShareSubscribeArgs, U, E](
    FlowableNode[S, A, U, E]
):
    id: int

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: A,  # SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        if self not in args.p_async_share_observers:
            # todo: handle this case
            raise Exception("not good")

        share_observer: AsyncShareObserver[U, E] = args.p_async_share_observers[self]
        shared = share_observer.shared

        transition = SubscribeTransition(child=shared.transition)

        with shared.transition_lock:
            shared.transition = transition

        match p_state := transition.get_state():
            case ActiveState(counter=id):
                shared.virtual_count_partition[id] = args.p_virtual_count
                shared.async_index_partition[id] = args.p_async_index

                requested_observer = AsyncShareRequestObserver(
                    id=id,
                    downstream=observer,
                    shared=shared,
                )
                share_observer.request_observers[id] = requested_observer

                cert = shared.async_translation.increase(
                    virtual_count=args.p_virtual_count,
                    async_index=args.p_async_index,
                    stacks=tuple(),
                    lambda_trace_logger=state.p_lambda_trace_logger,
                )

                return state, SubscriptionResult(
                    certificate=cert,
                    cancellable=AsyncShareCancellable(id=id, shared=shared),
                )

            case ErrorState():
                raise Exception()


class init_async_share_subject[S: State, U, E]:
    def __new__(cls, id: int) -> AsyncShareSubject[S, U, E]:
        return AsyncShareSubject(
            id=id,
        )
