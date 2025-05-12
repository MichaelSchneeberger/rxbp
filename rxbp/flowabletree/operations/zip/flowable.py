from __future__ import annotations
from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc
from donotation import do

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.operations.zip.states import AwaitUpstreamStateMixin
from rxbp.flowabletree.operations.zip.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.cancellable import ZipCancellable
from rxbp.flowabletree.operations.zip.observer import ZipObserver


@dataclassabc(frozen=True)
class ControlledZipFlowableNode[A, U](MultiChildrenFlowableNode[U, tuple[U, ...]]):
    children: tuple[FlowableNode, ...]
    func: Callable[[A, dict[int, U]], tuple[A, tuple[int, ...], U]]
    initial: A

    @do()
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[tuple[U, ...]]
    ) -> tuple[State, SubscriptionResult]:        
        shared = ZipSharedMemory[A, U](
            lock=Lock(),
            downstream=args.observer,
            zip_func=self.func,
            n_children=len(self.children),
            transition=None,  # type: ignore
            cancellables=None,  # type: ignore
            acc=self.initial,
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
        shared.cancellables = dict(cancellables)

        cancellable = ZipCancellable(
            cancellables=tuple(cancellables),
            shared=shared,
        )

        return state, SubscriptionResult(
            cancellable=cancellable,
            # cancellable=shared,
            certificate=certificate,
        )


def init_controlled_zip_flowable_node[A, U](
    children: tuple[FlowableNode[U], ...],
    func: Callable[[A, dict[int, U]], tuple[A, tuple[int, ...], U]],
    initial: A, 
):
    return ControlledZipFlowableNode[A, U](
        children=children,
        func=func,
        initial=initial,
)


def init_zip_flowable_node[A, U](
    children: tuple[FlowableNode[U], ...],
):
    def zip_func(acc: A, val: dict[int, U]):
        _, zipped_values = zip(*sorted(val.items()))
        return acc, tuple(), zipped_values

    return ControlledZipFlowableNode[A, U](
        children=children,
        func=zip_func,
        initial=None,
)
