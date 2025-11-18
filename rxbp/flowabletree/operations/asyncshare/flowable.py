from __future__ import annotations
from threading import Lock

from dataclassabc import dataclassabc
from donotation import do

from continuationmonad.scheduler.continuationcertificate import (
    ContinuationCertificateImpl,
)
from continuationmonad.utils.framesummary import get_frame_summary

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.asyncshare.states import ActiveState
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    SubscriptionTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.asyncshare.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.asyncshare.cancellable import ShareCancellation
from rxbp.flowabletree.operations.asyncshare.ackobserver import ShareAckObserver
from rxbp.flowabletree.operations.asyncshare.observer import SharedObserver


@dataclassabc(frozen=True)
class CancellableAsyncFlowableNode[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[U]
    ) -> tuple[State, SubscriptionResult]:
        assert self in state.async_shared_observers

        shared = ShareSharedMemory(
            # subscribe_certificate=None,
            upstream_cancellation=None,  # type: ignore
            deferred_handler=None,  # type: ignore
            lock=Lock(),
            transition=ToStateTransition(
                ActiveState(
                    subscription_cnt=0,
                    buffer_map=ActiveState.BufferMap(
                        mapping={},
                        next_index=0,
                        first_index=0,
                    ),
                    is_completed=False,
                    is_outer_cancelled=False,
                    action=ActiveState.ActionEnum(),
                    _downstream_certificates={},
                    _upstream_certificate=None,
                 )
            ),
            buffer_lock=Lock(),
            first_index=0,
            buffer=[],
        )

        observer = SharedObserver(
            shared=shared,
            weight=args.weight,
            # ack_observers={},
            # cancellations={},
        )

        # certificate = ContinuationCertificateImpl(
        #     lock=Lock(),
        #     weight=args.weight,
        #     stack=get_frame_summary(),
        #     validated=False,
        # )
        # state = state.copy(
        #     async_shared_observers=state.async_shared_observers | {self: observer},
        # )

        # state.async_shared_observers[self] = observer


@dataclassabc(frozen=True)
class AsyncShareFlowableNode[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode
    async_: AsyncFlowableNode

    def discover(
        self,
        state: State,
    ):
        # if self not in state.shared_subscribe_count:
        #     state.shared_subscribe_count[self] = 1
        #     state = self.child.discover(state)
        #
        # else:
        #     state.shared_subscribe_count[self] += 1

        return state

    def assign_weights(
        self,
        state: State,
        weight: int,
    ):
        # state.shared_subscribe_count[self] -= 1
        #
        # # the first assign_weights call
        # if self not in state.shared_weights:
        #     state.shared_weights[self] = weight
        #
        # else:
        #     state.shared_weights[self] += weight
        #
        # if state.shared_subscribe_count[self] == 0:
        #     state = self.child.assign_weights(state, state.shared_weights[self])

        return state

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[U]
    ) -> tuple[State, SubscriptionResult]:
        # asnyc_shared = AsyncFlowableNode(child=self.child)

        observer = state.async_shared_observers[self]
        shared = observer.shared

        transition = SubscriptionTransition(
            child=None,
            weight=args.weight,
        )

        with state.async_shared_lock:
            transition.child = state.async_shared_observers
            state.async_shared_observers = transition

        # observer = None
        # with state.async_shared_lock:
        #     if self in state.async_shared_observers:
        #         observer = state.async_shared_observers[self]
        #
        # do_subscribe = False
        #
        # if observer is None:
        #     shared = ShareSharedMemory(
        #         # subscribe_certificate=None,
        #         upstream_cancellation=None,
        #         deferred_handler=None,  # type: ignore
        #         lock=Lock(),
        #         transition=ToStateTransition(
        #             InitState(
        #                 weights={0: args.weight},
        #                 # buffer_map={0: 0},
        #                 # first_buffer_index=0,
        #                 # last_buffer_index=-1,
        #                 requested_certificates={},
        #                 # is_completed=False,
        #             )
        #         ),
        #         buffer_lock=Lock(),
        #         first_index=0,
        #         buffer=[],
        #     )
        #
        #     observer = SharedObserver(
        #         shared=shared,
        #         weight=args.weight,
        #         ack_observers={},
        #         cancellations={},
        #     )
        #
        #     with state.async_shared_lock:
        #         if self in state.async_shared_observers:
        #             state.async_shared_observers[self] = observer
        #             do_subscribe = True
        #
        #         else:
        #             observer = state.async_shared_observers[self]

        # state = state.copy(
        #     async_shared_observers=state.async_shared_observers
        #     | {self.async_: observer}
        # )
        # state.shared_observers[self] = observer

        # shared = observer.shared
        #
        # if do_subscribe:
        #     state, result = self.child.unsafe_subscribe(
        #         state,
        #         args.copy(
        #             observer=observer,
        #             weight=args.weight,
        #         ),
        #     )
        #     shared.upstream_cancellation = result.cancellable
        #     certificate = result.certificate
        #
        # else:
        #     certificate = ContinuationCertificateImpl(
        #         lock=Lock(),
        #         weight=args.weight,
        #         stack=get_frame_summary(),
        #         validated=False,
        #     )

        # init_state = shared.transition.state

        # id = len(init_state.buffer_map)
        # init_state.buffer_map[id] = 0
        # certificate, shared.subscribe_certificate = shared.subscribe_certificate.take(
        #     args.weight
        # )
        # init_state.requested_certificates[id] = None
        # shared.weights[id] = args.weight

        # id = 0

        downstream_cancellation = ShareCancellation(
            id=id,
            shared=shared,
            upstream_cancellation=shared.upstream_cancellation,
            _certificate=None,
        )

        ack_observer = ShareAckObserver(
            shared=shared,
            observer=args.observer,
            id=id,
            cancellation=downstream_cancellation,
            weight=args.weight,
        )

        observer.ack_observers[id] = ack_observer

        # assert isinstance(shared.transition.state, InitState)

        return state, SubscriptionResult(
            cancellable=downstream_cancellation,
            certificate=result.certificate,
        )


def init_share_flowable_node[V](child: FlowableNode[V]):
    return ShareFlowableNode[V](child=child)
