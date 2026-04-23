
from __future__ import annotations
from abc import ABC, abstractmethod
from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import Any, Self, override

import contcert

from rxbp.flowabletree.operations.asyncshare.cancellable import AsyncShareCancellation
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.asyncshare.requestobserver import AsyncShareRequestObserver
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    InitialTransition,
    SubscribeTransition,
)
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.asyncshare.sharedmemory import AsyncShareSharedMemory
from rxbp.flowabletree.operations.asyncshare.observer import AsyncShareObserver
from rxsingle.rxsingletree.state import State



class AsyncShareFlowableState(State, ABC):
    @property
    @abstractmethod
    def p_async_share_observers_lock(self) -> Lock: ...

    @property
    @abstractmethod
    def p_async_share_observers(self) -> dict[AsyncShareKey[Self, Any], AsyncShareObserver[Any]]: ...  # pyright: ignore[reportExplicitAny]


    # @abstractmethod
    # def update_async_share_observers[U](
    #     self,
    #     async_share_observers: tuple[AsyncShareSubject[Self, U], AsyncShareObserver[U]] | None = None,
    # ) -> Self: ...


@dataclass(frozen=True)
class AsyncShareKey[S, U]:
    child: FlowableNode[S, U]
    # context: int


@dataclass
class AsyncShareSubject[S: AsyncShareFlowableState, U](SingleChildFlowableNode[S, U, U]):
    key: None | AsyncShareKey[S, U]
    context: int
    # observer: AsyncShareObserver[U]
    # shared: AsyncShareSharedMemory[U]

    @override
    def unsafe_subscribe(
        self, state: S, args: SubscribeArgs[U]
    ) -> tuple[S, SubscriptionResult]:
        match self.key:

            case None:
                raise Exception("Not good")

            case AsyncShareKey():
                observer = state.p_async_share_observers[self.key]
                shared = observer.shared

                id = len(observer.request_observers)

                with shared.transition_lock:
                    shared.transition = SubscribeTransition(child=shared.transition, id=id)

                shared.virtual_count_partition[id] = args.virtual_count
                shared.context_partition[id] = args.context

                requested_observer = AsyncShareRequestObserver(
                    id=id,
                    downstream=args.observer,
                    shared=shared,
                    virtual_count=args.virtual_count,
                    context=args.context,

                )
                observer.request_observers[id] = requested_observer

                cancellable = AsyncShareCancellation(
                    id=id,
                    shared=shared
                )

                cert = shared.context_translation.increase(
                    virtual_count=args.virtual_count,
                    context=args.context,
                    stacks=tuple(),
                    lambda_trace_logger=state.p_lambda_trace_logger,
                )

                return state, SubscriptionResult(
                    certificate=cert,
                    cancellable=cancellable,
                )

    @property
    @override
    def p_child(self) -> FlowableNode[S, U]:
        return self.key.child
