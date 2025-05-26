from __future__ import annotations

from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc
from donotation import do

from rxbp.utils.framesummary import FrameSummary, FrameSummaryMixin
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import SingleChildFlowableNode, FlowableNode
from rxbp.flowabletree.operations.flatmap.cancellable import FlatMapCancellable
from rxbp.flowabletree.operations.flatmap.states import InitState
from rxbp.flowabletree.operations.flatmap.statetransitions import ToStateTransition
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.observer import FlatMapObserver


@dataclassabc(frozen=True, repr=False)
class FlatMapFlowableNode[U, V](FrameSummaryMixin, SingleChildFlowableNode[U, V]):
    child: FlowableNode[U]
    func: Callable[[U], FlowableNode[V]]
    stack: tuple[FrameSummary, ...]

    @do()
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs,
    ) -> tuple[State, SubscriptionResult]:
        shared = FlatMapSharedMemory(
            downstream=args.observer,
            lock=Lock(),
            transition=ToStateTransition(
                state=InitState(
                    # n_completed=0,
                    # n_children=0,
                    active_ids=tuple(),
                    certificates=tuple(),
                    is_outer_completed=False,
                ),
            ),
            cancellables={},
            upstream_cancellable=None,
        )

        state, result = self.child.unsafe_subscribe(
            state=state, args=args.copy(observer=FlatMapObserver(
                shared=shared,
                lock=Lock(),
                last_id=0,
                weight=args.weight,
                func=self.func,
                scheduler=args.scheduler,
                stack=self.stack,
                raise_immediately=state.raise_immediately,
            ))
        )

        shared.upstream_cancellable = result.cancellable

        return state, SubscriptionResult(
            cancellable=FlatMapCancellable(
                shared=shared,
            ),
            certificate=result.certificate,
        )


def init_flat_map_node[U, V](
    child: FlowableNode[U], 
    func: Callable[[U], FlowableNode[V]],
    stack: tuple[FrameSummary, ...],
):
    return FlatMapFlowableNode[U, V](child=child, func=func, stack=stack)
