from __future__ import annotations
from abc import ABC, abstractmethod
from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import Any, Self, override

import contcert

from rxbp.flowabletree.operations.asyncshare.subject import (
    AsyncShareFlowableState,
    AsyncShareKey,
    AsyncShareSubject,
)
from rxbp.flowabletree.operations.asyncshare.requestobserver import (
    AsyncShareRequestObserver,
)
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


@dataclass(frozen=True)
class ShareFlowableNode[S: AsyncShareFlowableState, U](
    SingleChildFlowableNode[S, U, U]
):
    child: FlowableNode[S, U]
    subject: AsyncShareSubject[S, U]
    # context: int
    key: AsyncShareKey[S, U]

    @override
    def unsafe_subscribe(
        self, state: S, args: SubscribeArgs[U]
    ) -> tuple[S, SubscriptionResult]:

        if self.key not in state.p_async_share_observers:
            shared = AsyncShareSharedMemory[U](
                virtual_count_partition={},
                context_partition={},
                # virtual_count=state.p_share_virtual_count[self],
                # virtual_count=self.virtual_count,
                context=self.subject.context,
                upstream=None,  # pyright: ignore[reportArgumentType]
                cancellable=None,  # pyright: ignore[reportArgumentType]
                context_translation=None,  # pyright: ignore[reportArgumentType]
                # cert_lock=Lock(),
                # cert=None,
                transition_lock=Lock(),
                transition=InitialTransition(),
                buffer_lock=Lock(),
                first_index=0,
                buffer=deque(),
                stacks=tuple(),
                lambda_trace_logger=state.p_subscription_trampoline.lambda_trace_logger,
            )

            observer = AsyncShareObserver(
                shared=shared,
                request_observers={},
                cancellations={},
            )

            with state.p_async_share_observers_lock:
                state.p_async_share_observers[self.key] = observer

            n_state, result = self.child.unsafe_subscribe(
                args=args.copy(
                    observer=observer,
                    virtual_count=1,
                    context=shared.context,
                ),
                state=state,
            )

            # shared.cert = result.certificate
            shared.context_translation = contcert.init_context_translation(
                cert=result.certificate,
                lambda_trace_logger=state.p_subscription_trampoline.lambda_trace_logger,
            )
            shared.cancellable = result.cancellable

            with shared.transition_lock:
                self.subject.key = self.key
                shared.transition = InitialTransition()

        else:
            n_state = state

        return self.subject.unsafe_subscribe(args=args, state=n_state)

    @property
    @override
    def p_child(self) -> FlowableNode[S, U]:
        return self.child


def init_async_share_flowable_node[S: AsyncShareFlowableState, U](
    child: FlowableNode[S, U],
    subject: AsyncShareSubject[S, U],
    # context: int,
):

    key = AsyncShareKey(child=child) #, context=context)
    subject.key = key

    return ShareFlowableNode[S, U](
        child=child,
        subject=subject,
        # subject=AsyncShareSubject(key=key, context=context),
        # context=context,
        key=key,
    )
