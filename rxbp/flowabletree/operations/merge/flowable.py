from __future__ import annotations

from itertools import accumulate

from dataclassabc import dataclassabc
from donotation import do

from continuationmonad.typing import (
    ContinuationCertificate,
)

from rxbp.cancellable import Cancellable
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.merge.states import UpstreamID
from rxbp.flowabletree.operations.merge.actions import InitAction
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.cancellable import MergeCancellable
from rxbp.flowabletree.operations.merge.observer import MergeObserver


@dataclassabc(frozen=True)
class Merge[V](MultiChildrenFlowableNode[V]):
    children: tuple[FlowableNode, ...]

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs,
    ) -> tuple[State, ObserveResult]:
        shared_state = MergeSharedMemory(
            downstream=args.observer,
            n_children=len(self.children),
            lock=state.lock,
            action=None,  # type: ignore
        )

        def acc_continuations(
            acc: tuple[
                State,
                list[ContinuationCertificate],
                list[tuple[UpstreamID, Cancellable]],
            ],
            value: tuple[int, FlowableNode],
        ):
            state, certificates, cancellables = acc
            id, child = value

            n_state, n_result = child.unsafe_subscribe(
                state,
                SubscribeArgs(
                    observer=MergeObserver(
                        shared=shared_state,
                        id=id,
                    ),
                    schedule_weight=args.schedule_weight
                )
            )

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append((id, n_result.cancellable))

            return n_state, certificates, cancellables

        *_, (n_state, (first_certificate, *other_certificates), cancellable_pairs) = (
            accumulate(
                func=acc_continuations,
                iterable=enumerate(self.children),
                initial=(state, [], []),
            )
        )

        shared_state.action = InitAction(
            n_completed=0,
            certificates=tuple(other_certificates),
        )

        cancellable = MergeCancellable(
            _certificate=None,
            n_children=shared_state.n_children,
            cancellables=dict(cancellable_pairs),
            shared=shared_state,
        )

        return n_state, ObserveResult(
            cancellable=cancellable, 
            certificate=first_certificate,
        )


def init_merge[V](children: tuple[FlowableNode[V], ...]):
    return Merge[V](children=children)
