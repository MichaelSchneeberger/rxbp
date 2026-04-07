from __future__ import annotations

from threading import Lock
from typing import override

from dataclasses import dataclass

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.buffer.observer import BufferObserver
from rxbp.flowabletree.operations.buffer.states import LoopInactive
from rxbp.flowabletree.operations.buffer.statetransitions import ToStateTransition


@dataclass(frozen=True)
class BufferImpl[V](SingleChildFlowableNode[V, V]):
    child: FlowableNode[V]
    buffer_size: int | None
    
    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ):
        loop_cancellation = init_cancellation_state()

        observer = BufferObserver(
            transition=None,
            upstream_cancellable=None,
            lock=Lock(),
            observer=args.observer,
            loop_cancellation=loop_cancellation,
            virtual_count=args.virtual_count,
            buffer=[],
            buffer_size=self.buffer_size,
        )

        state, result = self.child.unsafe_subscribe(
            state=state, 
            args=args.copy(
                observer=observer,
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
    buffer_size: int | None = None,
):
    return BufferImpl(
        child=child,
        buffer_size=buffer_size,
    )
