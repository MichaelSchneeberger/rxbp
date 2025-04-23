from __future__ import annotations

from typing import override

from dataclassabc import dataclassabc

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.buffer.loop import Loop
from rxbp.flowabletree.operations.buffer.observer import BufferObserver
from rxbp.flowabletree.operations.buffer.states import LoopInactive
from rxbp.flowabletree.operations.buffer.transitions import ToStateTransition


@dataclassabc(frozen=True)
class BufferImpl[V](SingleChildFlowableNode[V, V]):
    child: FlowableNode[V]
    
    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ): # -> tuple[State, ObserveResult]:
        # loop = Loop(
        #     observer=args.observer,
        #     transition=None,
        #     lock=state.lock,
        #     buffer=[]
        # )

        # upstream_cancellable = init_cancellation_state()
        loop_cancellation = init_cancellation_state()

        observer = BufferObserver(
            transition=None,
            upstream_cancellable=None,
            lock=state.lock,
            observer=args.observer,
            loop_cancellation=loop_cancellation,
            weight=args.schedule_weight,
            buffer=[],
        )

        state, result = self.child.unsafe_subscribe(
            state=state, 
            args=SubscribeArgs(
                observer=observer,
                schedule_weight=args.schedule_weight,
            ),
        )

        observer.upstream_cancellable = result.cancellable
        observer.transition = ToStateTransition(state=LoopInactive())

        return state, SubscriptionResult(
            certificate=result.certificate,
            cancellable=observer,
        )
    

    
def init_buffer[V](
    child: FlowableNode[V],
):
    return BufferImpl(
        child=child,
    )
