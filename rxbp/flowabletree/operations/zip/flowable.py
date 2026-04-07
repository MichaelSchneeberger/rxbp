from __future__ import annotations
from collections import deque
from threading import Lock
from typing import Callable, override

from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.zip.states import Active
from rxbp.flowabletree.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.zip.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.cancellable import ZipCancellable
from rxbp.flowabletree.operations.zip.observer import ZipObserver


@dataclass
class ControlledZipFlowableNode[S: State, U, A](MultiChildrenFlowableNode[S, U, A]):
    children: tuple[FlowableNode[S, U], ...]
    zip_func: Callable[[dict[int, U], A], tuple[A, set[int]]]
    acc: A

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[A],
    ) -> tuple[S, SubscriptionResult]:
        shared = ZipSharedMemory[U, A](
            downstream=args.observer,
            zip_func=self.zip_func,
            acc=self.acc,
            n_children=len(self.children),
            cancellables=None,  # pyright: ignore[reportArgumentType]
            transition=None,   # pyright: ignore[reportArgumentType]
            transition_lock=Lock(),
            certs=[],
            certs_lock=Lock(),
            items=deque(),
            upstream=deque(),
        )

        certificates: list[ContCert] = []
        cancellables: dict[int, Cancellable] = {}

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

            cancellables[id] = n_result.cancellable

        cert, *certs = certificates

        n_children = len(self.children)

        shared.transition = ToStateTransition(
            state=Active(
                requested_ids=set(range(n_children)),
                completed_ids=set(),
            )
        )
        shared.cancellables = dict(cancellables)
        shared.certs = certs

        cancellable = ZipCancellable(
            cancellables=tuple(cancellables.values()),
            shared=shared,
        )

        return state, SubscriptionResult(
            cancellable=cancellable,
            certificate=cert,
        )

    @property
    @override
    def p_children(self) -> tuple[FlowableNode[S, U], ...]:
        return self.children


# def init_controlled_zip_flowable_node[A, U](
#     children: tuple[FlowableNode[S, U], ...],
#     func: Callable[[A, dict[int, U]], tuple[A, tuple[int, ...], U]],
#     initial: A, 
# ):
#     return ControlledZipFlowableNode[A, U](
#         children=children,
#         func=func,
#         initial=initial,
# )


def init_zip_flowable_node[S: State, U](
    children: tuple[FlowableNode[S, U], ...],
):
    def zip_func(val: dict[int, U], acc: tuple[U, ...]):
        _, zipped_values = zip(*sorted(val.items()))
        return zipped_values, set(range(len(children)))

    return ControlledZipFlowableNode[S, U, tuple[U, ...]](
        children=children,
        zip_func=zip_func,
        acc=None,  # pyright: ignore[reportArgumentType]
)
