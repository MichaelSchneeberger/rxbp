from __future__ import annotations

from threading import Lock
from typing import Callable

from dataclasses import dataclass

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.concatmap.states import ActiveState
from rxbp.flowabletree.operations.concatmap.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.concatmap.sharedmemory import ConcatMapSharedMemory
from rxbp.flowabletree.operations.concatmap.cancellable import ConcatMapCancellable
from rxbp.flowabletree.operations.concatmap.observer import ConcatMapObserver



@dataclass(frozen=True)
class ConcatMapFlowable[U, V](SingleChildFlowableNode[U, V]):
    child: FlowableNode[S, U]
    func: Callable[[U], FlowableNode[V]]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        transition = ToStateTransition(
            state=ActiveState(
                cancellable=None,  # type: ignore
            ),
        )

        shared = ConcatMapSharedMemory(
            upstream_cancellable=None,
            transition=transition,
            lock=Lock(),
        )

        n_state, result = self.child.unsafe_subscribe(
            state=state, 
            args=args.copy(
                observer=ConcatMapObserver(
                    downstream=args.observer,
                    func=self.func,
                    scheduler=args.scheduler,
                    shared=shared,
                    virtual_count=args.virtual_count,
                ),
            ), # type: ignore
        )

        shared.upstream_cancellable = result.cancellable

        cancellable = ConcatMapCancellable(
            shared=shared,
        )

        return n_state, SubscriptionResult(
            cancellable=cancellable,
            certificate=result.certificate,
        )


def init_concat_map[U, V](
    child: FlowableNode[S, U],
    func: Callable[[U], FlowableNode[V]],
):
    return ConcatMapFlowable(
        child=child,
        func=func,
    )
