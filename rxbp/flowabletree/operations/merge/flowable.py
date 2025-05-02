from __future__ import annotations

from threading import Lock

from dataclassabc import dataclassabc
from donotation import do

from rxbp.cancellable import Cancellable
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.merge.states import SubscribedState
from rxbp.flowabletree.operations.merge.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.observer import MergeObserver


@dataclassabc(frozen=True)
class Merge[U](MultiChildrenFlowableNode[U, U]):
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

        for id, child in enumerate(self.children):
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
            state=SubscribedState(
                n_completed=0,
                n_children=len(self.children),
                certificates=tuple(others),
            ),
        )
        shared.cancellables = dict(cancellables)

        return n_state, SubscriptionResult(
            cancellable=shared,
            certificate=certificate,
        )


def init_merge[U](children: tuple[FlowableNode[U], ...]):
    return Merge[U](children=children)
