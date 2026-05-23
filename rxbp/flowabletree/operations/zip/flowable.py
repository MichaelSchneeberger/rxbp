from __future__ import annotations
from collections import deque
from threading import Lock
from typing import Any, Callable, override

from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.states import ActiveState
from rxbp.flowabletree.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.zip.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.cancellable import ZipCancellable
from rxbp.flowabletree.operations.zip.observer import ZipObserver


@dataclass(frozen=True)
class ControlledZipFlowableNode[S: State, A: SubscribeArgs, U, V, E](MultiChildrenFlowableNode[S, A, U, V, E]):
    children: tuple[FlowableNode[S, U, E], ...]
    zip_func: Callable[[dict[int, U], V], tuple[V, set[int]]]
    acc: V

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[V, E],
        args: A,
    ) -> tuple[S, SubscriptionResult]:
        shared = ZipSharedMemory[U, V, E](
            downstream=observer,
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
        cancellables: list[tuple[int, Cancellable]] = []

        for id, child in enumerate(self.children):
            state, n_result = child.unsafe_subscribe(
                state=state,
                observer=ZipObserver(
                    shared=shared,
                    id=id,
                ),
                args=args,
            )

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append((id, n_result.cancellable))

        cert, *certs = certificates

        n_children = len(self.children)

        shared.transition = ToStateTransition(
            state=ActiveState(
                requested_ids=set(range(n_children)),
                completed_ids=set(),
            )
        )
        shared.cancellables = dict(cancellables)
        shared.certs = certs

        cancellable = ZipCancellable(
            cancellables=tuple(dict(cancellables).values()),
            shared=shared,
        )

        return state, SubscriptionResult(
            cancellable=cancellable,
            certificate=cert,
        )

    @property
    @override
    def p_children(self) -> tuple[FlowableNode[S, U, E], ...]:
        return self.children


def init_zip_flowable_node[S: State, A, U, E](
    children: tuple[FlowableNode[S, A, U, E], ...],
):
    def zip_func(val: dict[int, U], acc: tuple[U, ...]):
        _, zipped_values = zip(*sorted(val.items()))
        return tuple(zipped_values), set(range(len(children)))

    return ControlledZipFlowableNode[S, U, A, tuple[U, ...], E](
        children=children,
        zip_func=zip_func,
        acc=None,  # pyright: ignore[reportArgumentType]
)
