from __future__ import annotations
from threading import Lock

from dataclassabc import dataclassabc
from donotation import do

from rxbp.flowabletree.operations.zip.states import AwaitUpstreamStateMixin
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.zip.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.cancellable import ZipCancellable
from rxbp.flowabletree.operations.zip.observer import ZipObserver



@dataclassabc(frozen=True)
class ZipFlowable[U](MultiChildrenFlowableNode[U, tuple[U, ...]]):
    children: tuple[FlowableNode, ...]

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[tuple[U, ...]]
    ) -> tuple[State, SubscriptionResult]:
        def zip_func(_: dict[int, U]):
            return tuple()

        shared = ZipSharedMemory(
            lock=Lock(),
            downstream=args.observer,
            zip_func=zip_func,
            n_children=len(self.children),
            transition=None,  # type: ignore
            cancellables=None,  # type: ignore
        )

        certificates = []
        cancellables = []

        for id, child in enumerate(self.children):

            state, n_result = child.unsafe_subscribe(
                state, 
                args=args.copy(
                    observer=ZipObserver(
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
            state=AwaitUpstreamStateMixin(
                certificates=tuple(others),
                values={},
                observers={},
                is_completed=False,
            )
        )
        shared.cancellables=dict(cancellables)

        # cancellable = ZipCancellable(
        #     cancellables=tuple(cancellables),
        #     shared=shared,
        # )

        return state, SubscriptionResult(
            # cancellable=cancellable, 
            cancellable=shared, 
            certificate=certificate,
        )


def init_zip[V](children: tuple[FlowableNode[V], ...]):
    return ZipFlowable[V](children=children)
