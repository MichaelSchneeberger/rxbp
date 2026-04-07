from __future__ import annotations
from threading import Lock

from dataclasses import dataclass
from donotation import do

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.share.states import InitState
from rxbp.flowabletree.operations.share.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.share.ackobserver import ShareAckObserver
from rxbp.flowabletree.operations.share.observer import SharedObserver


@dataclass(frozen=True)
class ShareFlowableNode[S, U](SingleChildFlowableNode[S, U, U]):
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

    def assign_virtual_count(
        self,
        state: State,
        virtual_count: int,
    ):
        state.shared_subscribe_count[self] -= 1

        if self not in state.shared_virtual_count:
            state.shared_virtual_count[self] = virtual_count

        else:
            state.shared_virtual_count[self] += virtual_count

        if state.shared_subscribe_count[self] == 0:
            state = self.child.assign_virtual_count(state, state.shared_virtual_count[self])

        return state

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[U]
    ) -> tuple[State, SubscriptionResult]:
        if self in state.shared_observers:
            observer = state.shared_observers[self]

        else:
            total_virtual_count = state.shared_virtual_count[self]

            shared = ShareSharedMemory(
                upstream_cancellation=None,
                transition=None,  # type: ignore
                deferred_handler=None,  # type: ignore
                lock=Lock(),
                buffer_lock=Lock(),
                first_index=0,
                buffer=[],
                total_virtual_count=total_virtual_count,
                virtual_count_partition={},
                init_certificate=None,
            )

            observer = SharedObserver(
                shared=shared,
                virtual_count=args.virtual_count,
                ack_observers={},
                cancellations={},
            )

            state.shared_observers[self] = observer

            state, result = self.child.unsafe_subscribe(
                state,
                args.copy(
                    observer=observer,
                    virtual_count=total_virtual_count,
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
                    virtual_counts={},
                )
            )

        shared = observer.shared

        init_state = shared.transition.state

        id = len(init_state.buffer_map)
        init_state.buffer_map[id] = 0
        certificate, shared.init_certificate = shared.init_certificate.take(args.virtual_count)
        # print(certificate)
        init_state.requested_certificates[id] = None
        init_state.virtual_counts[id] = args.virtual_count

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
            # virtual_count=args.virtual_count,
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
