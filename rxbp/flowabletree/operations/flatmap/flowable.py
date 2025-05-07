from __future__ import annotations

from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc
from donotation import do

from rxbp.flowabletree.operations.flatmap.cancellable import FlatMapCancellable
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import SingleChildFlowableNode, FlowableNode
from rxbp.flowabletree.operations.flatmap.states import InitState
from rxbp.flowabletree.operations.flatmap.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.observer import FlatMapObserver


@dataclassabc(frozen=True)
class FlatMapFlowable[U, V](SingleChildFlowableNode[U, V]):
    child: FlowableNode[U]
    func: Callable[[U], FlowableNode[V]]

    @do()
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs,
    ) -> tuple[State, SubscriptionResult]:
        shared = FlatMapSharedMemory(
            downstream=args.observer,
            lock=Lock(),
            transition=ToStateTransition(
                state=InitState(),
            ),
            cancellables={},
        )

        state, result = self.child.unsafe_subscribe(
            state=state, args=args.copy(observer=FlatMapObserver(
                shared=shared,
                lock=Lock(),
                last_id=0,
                weight=args.schedule_weight,
                func=self.func,
                scheduler=state.scheduler,
                schedule_weight=args.schedule_weight,
            ))
        )

        cancellable = FlatMapCancellable(
            upstream=result.cancellable,
            shared=shared,
        )

        return state, SubscriptionResult(
            cancellable=cancellable,
            certificate=result.certificate,
        )


def init_flat_map[U, V](
    child: FlowableNode[U], 
    func: Callable[[U], FlowableNode[V]],
):
    return FlatMapFlowable[U, V](child=child, func=func)
