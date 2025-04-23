from __future__ import annotations

from dataclassabc import dataclassabc
from donotation import do

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.zip.transitions import RequestTransition
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.cancellable import CompositeCancellable
from rxbp.flowabletree.operations.zip.observer import ZipObserver



class Zip[V](MultiChildrenFlowableNode[V]):
    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[tuple[V, ...]]
    ) -> tuple[State, SubscriptionResult]:
        def zip_func(_: dict[int, V]):
            return tuple()

        shared_memory = ZipSharedMemory(
            lock=state.lock,
            downstream=args.observer,
            zip_func=zip_func,
            n_children=len(self.children),
            action=None,  # type: ignore
            cancellables=None,  # type: ignore
        )

        certificates = []
        cancellables = []

        for id, child in enumerate(self.children):

            n_args = args.copy(
                    observer=ZipObserver(
                    shared=shared_memory,
                    id=id,
                ),
            )

            state, n_result = child.unsafe_subscribe(state, n_args)

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append(n_result.cancellable)

        certificate, *others = certificates

        shared_memory.action = RequestTransition(
            certificates=tuple(others),
            values={},
            observers={}
        )
        shared_memory.cancellables = tuple(cancellables)

        cancellable = CompositeCancellable(
            cancellables=cancellables,
            certificates=tuple(others),
        )

        return state, SubscriptionResult(
            cancellable=cancellable, 
            certificate=certificate,
        )


@dataclassabc(frozen=True)
class ZipImpl[V](Zip[V]):
    children: tuple[FlowableNode, ...]


def init_zip[V](children: tuple[FlowableNode[V], ...]):
    return ZipImpl[V](children=children)
