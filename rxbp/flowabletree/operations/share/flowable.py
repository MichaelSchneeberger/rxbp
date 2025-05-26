from __future__ import annotations
from threading import Lock

from dataclassabc import dataclassabc
from donotation import do

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.share.states import InitState
from rxbp.flowabletree.operations.share.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.share.ackobserver import ShareAckObserver
from rxbp.flowabletree.operations.share.observer import SharedObserver


@dataclassabc(frozen=True)
class ShareFlowableNode[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode

    def discover(
        self, 
        state: State,
    ):
        if self not in state.shared_subscribe_count:
            state.shared_subscribe_count[self] = 1
            state = self.child.discover(state)

        else:
            state.shared_subscribe_count[self] += 1

        return state

    def assign_weights(
        self,
        state: State,
        weight: int,
    ):
        state.shared_subscribe_count[self] -= 1

        if self not in state.shared_weights:
            state.shared_weights[self] = weight

        else:
            state.shared_weights[self] += weight

        if state.shared_subscribe_count[self] == 0:
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
                transition=None,  # type: ignore
                deferred_handler=None,  # type: ignore
                lock=Lock(),
                buffer_lock=Lock(),
                first_index=0,
                buffer=[],
                total_weight=total_weight,
                weight_partition={},
                init_certificate=None,
            )

            observer = SharedObserver(
                shared=shared,
                weight=args.weight,
                ack_observers={},
                cancellations={},
            )

            state.shared_observers[self] = observer

            state, result = self.child.unsafe_subscribe(
                state,
                args.copy(
                    observer=observer,
                    weight=total_weight,
                ),
            )
            shared.upstream_cancellation = result.cancellable
            shared.init_certificate = result.certificate

            shared.transition = ToStateTransition(
                InitState(
                    buffer_map={},
                    first_buffer_index=0,
                    last_buffer_index=-1,
                    cancelled_certificates={},
                    requested_certificates={},
                    is_completed=False,
                    weights={},
                )
            )

        shared = observer.shared

        init_state = shared.transition.state

        id = len(init_state.buffer_map)
        init_state.buffer_map[id] = 0
        certificate, shared.init_certificate = shared.init_certificate.take(
            args.weight
        )
        # print(certificate)
        init_state.requested_certificates[id] = None
        init_state.weights[id] = args.weight

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
            weight=args.weight,
            cancellation=downstream_cancellation,
        )

        observer.ack_observers[id] = ack_observer

        assert isinstance(shared.transition.state, InitState)

        return state, SubscriptionResult(
            cancellable=downstream_cancellation,
            certificate=certificate,
        )


def init_share_flowable_node[V](child: FlowableNode[V]):
    return ShareFlowableNode[V](child=child)
