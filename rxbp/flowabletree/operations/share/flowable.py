from __future__ import annotations
from abc import ABC, abstractmethod
from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import Any, Self, override

import contcert

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.cancellable import ShareCancellation
from rxbp.flowabletree.operations.share.requestobserver import ShareRequestObserver
from rxbp.flowabletree.operations.share.statetransitions import (
    InitialTransition,
    SubscribeTransition,
)
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.observer import ShareObserver
from rxsingle.rxsingletree.state import State


class ShareFlowableState(State, ABC):
    @property
    @abstractmethod
    def p_share_flowable_count(self) -> dict[ShareFlowableNode[Self, Any, Any], int]: ...  # pyright: ignore[reportExplicitAny]

    @property
    @abstractmethod
    def p_share_virtual_count(self) -> dict[ShareFlowableNode[Self, Any, Any], int]: ...  # pyright: ignore[reportExplicitAny]

    """
    Dictionary containing all shared Flowable nodes that are encountered during the subscription process.
    It maps the shared Flowable node to its observer created when first subscribing.
    When subscribing multiple times to the same Flowable node, the observer is used that is stored in the
    dictionary.

    This dictionary is modified during the subscription of a Flowable.
    """

    @property
    @abstractmethod
    def p_share_observers(
        self,
    ) -> dict[ShareFlowableNode[Self, Any, Any], ShareObserver[Any, Any]]: ...  # pyright: ignore[reportExplicitAny]

    @abstractmethod
    def update_shared_flowable_data[U, E](
        self,
        flowable_count: dict[ShareFlowableNode[Self, U, E], int] | None = None,
        virtual_count: dict[ShareFlowableNode[Self, U, E], int] | None = None,
        share_observers: tuple[ShareFlowableNode[Self, U, E], ShareObserver[U, E]] | None = None,
    ) -> Self: ...


@dataclass(frozen=True)
class ShareFlowableNode[S: ShareFlowableState, A: SubscribeArgs, U, E](SingleChildFlowableNode[S, A, U, U, E]):
    child: FlowableNode[S, U, E]

    @override
    def discover(self, state: S) -> S:
        # copy dictionary
        flowable_count_map = dict(state.p_share_flowable_count)

        if self not in flowable_count_map:
            flowable_count_map[self] = 1
            state = state.update_shared_flowable_data(
                flowable_count=flowable_count_map,
            )
            state = self.child.discover(state)

        else:
            flowable_count_map[self] += 1
            state = state.update_shared_flowable_data(
                flowable_count=flowable_count_map,
            )

        return state

    @override
    def assign_virtual_count(
        self,
        state: S,
        virtual_count: int,
    ) -> S:
        flowable_count_map = dict(state.p_share_flowable_count)
        virtual_count_map = dict(state.p_share_virtual_count)

        flowable_count_map[self] -= 1

        if self not in virtual_count_map:
            virtual_count_map[self] = virtual_count
            state = state.update_shared_flowable_data(
                virtual_count=virtual_count_map,
                flowable_count=flowable_count_map,
            )

        else:
            virtual_count_map[self] += virtual_count
            state = state.update_shared_flowable_data(
                virtual_count=virtual_count_map,
                flowable_count=flowable_count_map,
            )

        if flowable_count_map[self] == 0:
            state = self.child.assign_virtual_count(state, virtual_count_map[self])

        return state

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: A,
    ) -> tuple[S, SubscriptionResult]:
        if self not in state.p_share_observers:
            # observer = state.p_share_observers[self]
            # shared = observer.shared

            # total_virtual_count = state.shared_virtual_count[self]

            shared = ShareSharedMemory[U, E](
                virtual_count_partition={},
                virtual_count=state.p_share_virtual_count[self],
                async_index=args.async_index,
                upstream=None,  # pyright: ignore[reportArgumentType]
                cancellable=None,  # pyright: ignore[reportArgumentType]
                cert_lock=Lock(),
                cert=None,
                transition_lock=Lock(),
                transition=InitialTransition(),
                buffer_lock=Lock(),
                first_index=0,
                buffer=deque(),
                stacks=tuple(),
                lambda_trace_logger=state.p_lambda_trace_logger,
            )

            share_observer = ShareObserver(
                shared=shared,
                request_observers={},
                cancellations={},
            )

            n_state, result = self.child.unsafe_subscribe(
                args=args.copy(
                    virtual_count=shared.virtual_count,
                ),
                observer=share_observer,
                state=state.update_shared_flowable_data(
                    share_observers=(self, share_observer)
                ),
            )

            shared.cert = result.certificate
            shared.cancellable = result.cancellable
            id = 0

        else:
            share_observer = state.p_share_observers[self]
            shared = share_observer.shared
            n_state = state


            id = len(share_observer.request_observers)
            shared.transition = SubscribeTransition(child=shared.transition, id=id)

        shared.virtual_count_partition[id] = args.virtual_count

        requested_observer = ShareRequestObserver(
            id=id,
            downstream=observer,
            shared=shared,
        )
        share_observer.request_observers[id] = requested_observer

        cancellable = ShareCancellation(
            id=id,
            shared=shared
        )

        cert, shared.cert = contcert.take(
            cert=shared.cert, 
            virtual_count=args.virtual_count,
            stacks=tuple(),
            lambda_trace_logger=state.p_lambda_trace_logger,
        )

        return n_state, SubscriptionResult(
            certificate=cert,
            cancellable=cancellable,
        )

    @property
    @override
    def p_child(self) -> FlowableNode[S, U, E]:
        return self.child

def init_share_flowable_node[S: ShareFlowableState, U, E](child: FlowableNode[S, U, E]):
    return ShareFlowableNode(child=child)
