from __future__ import annotations

from dataclassabc import dataclassabc
from donotation import do

from rxbp.exceptions import RxBpException
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.share.states import InitState
from rxbp.flowabletree.operations.share.transitions import ToStateTransition
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.share.ackobserver import ShareAckObserver
from rxbp.flowabletree.operations.share.observer import SharedObserver


@dataclassabc(frozen=True)
class ShareFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode

    def discover(
        self, 
        state: State,
    ):
        if self not in state.subscriber_count:
            state.subscriber_count[self] = 1
            state = self.child.discover(state)

        else:
            state.subscriber_count[self] += 1

        return state

    def assign_weights(
        self,
        state: State,
        weight: int,
    ):
        state.subscriber_count[self] -= 1
        if self not in state.shared_weights:
            state.shared_weights[self] = weight

        else:
            state.shared_weights[self] += weight

        if state.subscriber_count[self] == 0:
            state = self.child.assign_weights(state, state.shared_weights[self])

        return state

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[U]
    ) -> tuple[State, SubscriptionResult]:
        if self in state.shared_observers:
            observer = state.shared_observers[self]

        else:
            total_weight = state.shared_weights[self]

            shared = ShareSharedMemory(
                upstream_cancellation=None,
                action=None,  # type: ignore
                deferred_observer=None,  # type: ignore
                lock=state.lock,
                buffer_lock=state.lock,
                first_index=0,
                buffer=[],
                total_weight=total_weight,
            )

            observer = SharedObserver(
                shared=shared,
                weight=args.schedule_weight,
                ack_observers={},
                cancellations={},
            )

            state.shared_observers[self] = observer

            state, result = self.child.unsafe_subscribe(
                state,
                SubscribeArgs(
                    observer=observer,
                    schedule_weight=total_weight,
                ),
            )
            shared.upstream_cancellation = result.cancellable

            shared.action = ToStateTransition(
                InitState(
                    buffer_map={},
                    first_buffer_index=0,
                    last_buffer_index=-1,
                    acc_certificate=result.certificate,
                    is_ack=False,
                    is_completed=False,
                )
            )

        shared = observer.shared

        init_state = shared.action.state

        id = len(init_state.buffer_map)
        init_state.buffer_map[id] = 0
        certificate, acc_certificate = init_state.acc_certificate.take(
            args.schedule_weight
        )
        init_state.acc_certificate = acc_certificate

        downstream_cancellation = ShareCancellation(
            id=id,
            shared=shared,
            upstream_cancellation=shared.upstream_cancellation,
            _certificate=None,
        )

        ack_observer = ShareAckObserver(
            observer=args.observer,
            shared=shared,
            id=id,
            weight=args.schedule_weight,
            cancellation=downstream_cancellation,
        )

        observer.ack_observers[id] = ack_observer

        assert isinstance(shared.action.state, InitState)

        return state, SubscriptionResult(
            cancellable=downstream_cancellation,
            certificate=certificate,
        )


def init_share[V](child: FlowableNode[V]):
    return ShareFlowable[V](child=child)
