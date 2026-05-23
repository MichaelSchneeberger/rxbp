from __future__ import annotations

from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.state import State
from rxbp.flowabletree.operations.merge.cancellable import MergeCancellable
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.merge.states import ActiveState
from rxbp.flowabletree.operations.merge.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.observer import MergeObserver


@dataclass(frozen=True)
class MergeFlowableNode[S: State, A: SubscribeArgs, U, E](MultiChildrenFlowableNode[S, A, U, U, E]):
    children: tuple[FlowableNode[S, U, E], ...]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: A,
    ) -> tuple[S, SubscriptionResult]:
        shared = MergeSharedMemory(
            downstream=observer,
            cancellables=None,  # pyright: ignore[reportArgumentType]
            transition=None,   # pyright: ignore[reportArgumentType]
            transition_lock=Lock(),
            certs=[],
            certs_lock=Lock(),
            items=deque(),
            upstream=deque(),
        )

        certificates: list[ContCert] = []
        cancellables: list[tuple[int, Cancellable]] = []
        active_ids: set[int] = set()

        for id, child in enumerate(self.children):
            active_ids.add(id)

            state, n_result = child.unsafe_subscribe(
                state=state,
                observer=MergeObserver(
                    shared=shared,
                    id=id,
                ),
                args=args,
            )

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append((id, n_result.cancellable))

        # certificate, *others = certificates
        shared.certs = certificates[1:]
        shared.transition = ToStateTransition(
            state=ActiveState(
                active_ids=active_ids,
                received_ids=tuple(),
            ),
        )
        shared.cancellables = dict(cancellables)

        cancellable = MergeCancellable(shared=shared)

        return state, SubscriptionResult(
            cancellable=cancellable,
            certificate=certificates[0],
        )

    @property
    @override
    def p_children(self) -> tuple[FlowableNode[S, U, E], ...]:
        return self.children


def init_merge_flowable_node[S: State, A: SubscribeArgs, U, E](
        children: tuple[FlowableNode[S, A, U, E], ...],
):
    return MergeFlowableNode[S, A, U, E](
        children=children,
    )
