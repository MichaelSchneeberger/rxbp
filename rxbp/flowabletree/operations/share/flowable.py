from __future__ import annotations

from dataclassabc import dataclassabc
from donotation import do

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.share.states import InitState
from rxbp.flowabletree.operations.share.actions import FromStateAction
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.share.ackobserver import ShareAckObserver
from rxbp.flowabletree.operations.share.observer import SharedObserver


@dataclassabc(frozen=True)
class ShareFlowable[V](SingleChildFlowableNode[V, V]):
    child: FlowableNode

    def discover(self, subscriber_count: dict[FlowableNode, int]):
        if self not in subscriber_count:
            subscriber_count[self] = 1
            self.child.discover(subscriber_count)
        else:
            subscriber_count[self] += 1

    def assign_weights(
        self,
        weight: int,
        shared_weights: dict[FlowableNode, int],
        subscriber_count: dict[FlowableNode, int],
    ):
        if self not in shared_weights:
            shared_weights[self] = weight
        else:
            shared_weights[self] += weight
        subscriber_count[self] -= 1

        if subscriber_count[self] == 0:
            self.child.assign_weights(shared_weights[self], shared_weights, subscriber_count)

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[V]
    ) -> tuple[State, ObserveResult]:
        weight = state.shared_weights[self]

        if self in state.shared_observers:
            observer = state.shared_observers[self]

        else:
            shared = ShareSharedMemory(
                upstream_cancellation=None,
                action=None,  # type: ignore
                upstream_ack_observer=None,  # type: ignore
                lock=state.lock,
                buffer_lock=state.lock,
                first_index=0,
                buffer=[],
                total_weight=weight,
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
                    schedule_weight=weight,
                ),
            )
            shared.upstream_cancellation = result.cancellable

            shared.action = FromStateAction(
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
            downstream_observer=args.observer,
            shared=shared,
            id=id,
            weight=args.schedule_weight,
            cancellation=downstream_cancellation,
        )

        observer.ack_observers[id] = ack_observer

        assert isinstance(shared.action.state, InitState)

        return state, ObserveResult(
            cancellable=downstream_cancellation,
            certificate=certificate,
        )


def init_share[V](child: FlowableNode[V]):
    return ShareFlowable[V](child=child)
