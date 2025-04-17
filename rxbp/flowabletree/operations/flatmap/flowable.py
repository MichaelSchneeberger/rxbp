from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Callable

from dataclassabc import dataclassabc
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.flatmap.states import ActiveState
from rxbp.flowabletree.operations.flatmap.actions import FromStateAction
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.cancellable import FlatMapCancellable
from rxbp.flowabletree.operations.flatmap.observer import FlatMapObserver


@dataclass(frozen=True)
class FlatMap[V](SingleChildFlowableNode[V, V]):
    func: Callable[[V], FlowableNode]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, ObserveResult]:
        action = FromStateAction(
            state=ActiveState(
                cancellable=None,  # type: ignore
            ),
        )

        shared = FlatMapSharedMemory(
            upstream_cancellable=None,
            action=action,
            lock=RLock(),
        )

        n_state, result = self.child.unsafe_subscribe(
            state=state, 
            args=SubscribeArgs(
                observer=FlatMapObserver(
                    downstream=args.observer,
                    func=self.func,
                    scheduler=state.scheduler,
                    shared=shared,
                    schedule_weight=args.schedule_weight,
                ),
                schedule_weight=args.schedule_weight,
            ),
        )

        shared.upstream_cancellable = result.cancellable

        cancellable = FlatMapCancellable(
            _certificate=None, # type: ignore
            shared=shared,
        )

        return n_state, ObserveResult(
            cancellable=cancellable,
            certificate=result.certificate,
        )


@dataclassabc(frozen=True)
class FlatMapImpl[V](FlatMap[V]):
    child: FlowableNode[V]


def init_flat_map[V](
    child: FlowableNode[V],
    func: Callable[[V], FlowableNode],
):
    return FlatMapImpl(
        child=child,
        func=func,
    )
