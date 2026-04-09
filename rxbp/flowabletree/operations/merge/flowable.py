from __future__ import annotations

from collections import deque
from threading import Lock

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert
from rxbp.cancellable import Cancellable
from rxbp.flowabletree.state import State
from rxbp.flowabletree.operations.merge.cancellable import MergeCancellable
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.merge.states import Active, InitState
from rxbp.flowabletree.operations.merge.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.observer import MergeObserver


@dataclass(frozen=True)
class MergeFlowableNode[S: State, U](MultiChildrenFlowableNode[S, U, U]):
    children: tuple[FlowableNode[S, U], ...]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        shared = MergeSharedMemory(
            downstream=args.observer,
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
                args=args.copy(
                    observer=MergeObserver(
                        shared=shared,
                        id=id,
                    ),
                ),
            )

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append((id, n_result.cancellable))

        # certificate, *others = certificates
        shared.certs = certificates[1:]
        shared.transition = ToStateTransition(
            state=Active(
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
    def p_children(self) -> tuple[FlowableNode[S, U], ...]:
        return self.children


def init_merge_flowable_node[S: State, U](
        children: tuple[FlowableNode[S, U], ...],
):
    return MergeFlowableNode[S, U](
        children=children,
    )
