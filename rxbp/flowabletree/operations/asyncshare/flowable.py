from __future__ import annotations
from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import override

import contcert

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.asyncshare.cancellable import AsyncShareCancellable
from rxbp.flowabletree.operations.asyncshare.requestobserver import (
    AsyncShareRequestObserver,
)
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    InitialTransition,
)
from rxbp.flowabletree.operations.asyncshare.subject import (
    AsyncShareSubject,
    AsyncShareSubscribeArgs,
)
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.asyncshare.sharedmemory import AsyncShareSharedMemory
from rxbp.flowabletree.operations.asyncshare.observer import AsyncShareObserver
from rxbp.flowabletree.state import State


@dataclass(frozen=True)
class AsyncShareFlowableNode[S: State, A: AsyncShareSubscribeArgs, U, E](
    SingleChildFlowableNode[S, A, U, U, E]
):
    child: FlowableNode[S, A, U, E]
    subject: AsyncShareSubject[S, A, U, E]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: A,
    ) -> tuple[S, SubscriptionResult]:

        if self.subject in args.p_async_share_observers:
            # todo: handle case
            raise Exception('not good')

        share_observer = AsyncShareObserver[U, E](
            shared=None,  # pyright: ignore[reportArgumentType]
            request_observers={},
            cancellations={},
        )

        n_args = args.update_basics(
            # +1 virtual count required for virtual count translation
            virtual_count=args.p_virtual_count + 1,
            async_index=args.p_async_index,
        ).update_async_share_observers(
            observers=args.p_async_share_observers | {self.subject: share_observer},
        )

        n_state, result = self.child.unsafe_subscribe(
            args=n_args,
            observer=share_observer,
            state=state,
        )

        cert1, cert2 = contcert.take(
            result.certificate,
            1,
            stacks=tuple(),
            lambda_trace_logger=state.p_lambda_trace_logger,
        )
        # print(args.p_async_index)

        shared = AsyncShareSharedMemory[U, E](
            virtual_count_partition={},
            # virtual_count=args.virtual_count,
            async_index_partition={},
            async_index=args.p_async_index,
            upstream=None,
            cancellable=result.cancellable,
            async_translation=contcert.init_async_index_translation(
                cert=cert1,
                lambda_trace_logger=state.p_lambda_trace_logger,
            ),
            cert=contcert.empty_cert(
                async_index=args.p_async_index,
                stacks=tuple(),
            ),
            transition_lock=Lock(),
            transition=InitialTransition(),
            buffer_lock=Lock(),
            first_index=0,
            buffer=deque(),
            stacks=tuple(),
            lambda_trace_logger=state.p_lambda_trace_logger,
        )
        share_observer.shared = shared

        id = 0
        requested_observer = AsyncShareRequestObserver(
            id=id,
            downstream=observer,
            shared=shared,
        )
        share_observer.request_observers[id] = requested_observer

        return n_state, SubscriptionResult(
            certificate=cert2,
            cancellable=AsyncShareCancellable(
                id=id,
                shared=shared,
            ),
        )

    @property
    @override
    def p_child(self) -> FlowableNode[S, A, U, E]:
        return self.child


def init_async_share_flowable_node[S: State, A: AsyncShareSubscribeArgs, U, E](
    child: FlowableNode[S, A, U, E],
    subject: AsyncShareSubject[S, A, U, E],
):
    return AsyncShareFlowableNode(
        child=child,
        subject=subject,
    )
