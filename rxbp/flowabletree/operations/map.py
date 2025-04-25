from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from dataclassabc import dataclassabc

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class MapFlowable[U, V](SingleChildFlowableNode[U, V]):
    child: FlowableNode[U]
    func: Callable[[U], V]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        @dataclass
        class MapObserver(Observer):
            func: Callable[[U], V]

            def on_next(self, value: U):
                return args.observer.on_next(self.func(value))

            def on_next_and_complete(self, value: U):
                return args.observer.on_next_and_complete(self.func(value))

            on_completed = args.observer.on_completed
            on_error = args.observer.on_error

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=MapObserver(func=self.func),
                schedule_weight=args.schedule_weight,
            ),
        )


def init_map_flowable[U, V](
    child: FlowableNode[U],
    func: Callable[[U], V],
):
    return MapFlowable[U, V](
        child=child,
        func=func,
    )
