from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import continuationmonad
from dataclassabc import dataclassabc

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class ReduceFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    func: Callable[[U, U], U]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class ReduceObserver(Observer[U]):
            is_first: bool
            acc: U | None

            def on_next(self, item: U):
                if self.is_first:
                    self.is_first = False
                    self.acc = item

                else:
                    self.acc = outer_self.func(self.acc, item)
                
                return continuationmonad.from_(None)

            def on_next_and_complete(self, item: U):
                self.acc = outer_self.func(self.acc, item)
                return args.observer.on_next_and_complete(self.acc)

            def on_completed(self):
                return args.observer.on_next_and_complete(self.acc)

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=ReduceObserver(is_first=True, acc=None),
                weight=args.weight,
            ),
        )


def init_reduce_flowable[U](
    child: FlowableNode[U],
    func: Callable[[U, U], U],
):
    return ReduceFlowable[U](
        child=child,
        func=func,
    )
