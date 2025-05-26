from __future__ import annotations

from threading import Lock

from dataclassabc import dataclassabc
from donotation import do

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.merge.cancellable import MergeCancellable
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.merge.states import InitState
from rxbp.flowabletree.operations.merge.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.observer import MergeObserver


@dataclassabc(frozen=True)
class MergeFlowableNode[U](MultiChildrenFlowableNode[U, U]):
    children: tuple[FlowableNode, ...]

    @do()
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs,
    ) -> tuple[State, SubscriptionResult]:
        shared = MergeSharedMemory(
            downstream=args.observer,
            lock=Lock(),
            transition=None,  # type: ignore
            cancellables=None,
        )

        certificates = []
        cancellables: list[tuple[int, Cancellable]] = []
        active_ids = []

        for id, child in enumerate(self.children):
            active_ids.append(id)
            n_state, n_result = child.unsafe_subscribe(
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

        certificate, *others = certificates

        shared.transition = ToStateTransition(
            state=InitState(
                active_ids=tuple(active_ids),
                certificates=tuple(others),
            ),
        )
        shared.cancellables = dict(cancellables)

        cancellable = MergeCancellable(shared=shared)

        return n_state, SubscriptionResult(
            cancellable=cancellable,
            certificate=certificate,
        )


def init_merge_flowable_node[U](children: tuple[FlowableNode[U], ...]):
    return MergeFlowableNode[U](children=children)
