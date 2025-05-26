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
        outer_self = self

        @dataclass
        class MapObserver(Observer):
            def on_next(self, item: U):
                return args.observer.on_next(outer_self.func(item))

            def on_next_and_complete(self, item: U):
                return args.observer.on_next_and_complete(outer_self.func(item))

            def on_completed(self):
                return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

            # on_completed = args.observer.on_completed
            # on_error = args.observer.on_error

        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=MapObserver(),
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
