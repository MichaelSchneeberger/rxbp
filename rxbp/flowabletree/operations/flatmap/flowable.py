from __future__ import annotations

from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.flatmap.states import ActiveState
from rxbp.flowabletree.operations.flatmap.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.cancellable import FlatMapCancellable
from rxbp.flowabletree.operations.flatmap.observer import FlatMapObserver



@dataclassabc(frozen=True)
class FlatMapFlowable[U, V](SingleChildFlowableNode[U, V]):
    child: FlowableNode[U]
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

        shared = FlatMapSharedMemory(
            upstream_cancellable=None,
            transition=transition,
            lock=Lock(),
        )

        n_state, result = self.child.unsafe_subscribe(
            state=state, 
            args=SubscribeArgs(
                observer=FlatMapObserver(
                    downstream=args.observer,
                    func=self.func,
                    scheduler=state.scheduler,
                    shared=shared,
                    schedule_weight=args.schedule_weight,
                ),
                schedule_weight=args.schedule_weight,
            ),
        )

        shared.upstream_cancellable = result.cancellable

        cancellable = FlatMapCancellable(
            shared=shared,
        )

        return n_state, SubscriptionResult(
            cancellable=cancellable,
            certificate=result.certificate,
        )


def init_flat_map[U, V](
    child: FlowableNode[U],
    func: Callable[[U], FlowableNode[V]],
):
    return FlatMapFlowable(
        child=child,
        func=func,
    )
