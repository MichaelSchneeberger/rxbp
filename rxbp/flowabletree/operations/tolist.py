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
class ToListFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    size: int | None

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[list[U]],
    ) -> tuple[State, SubscriptionResult]:
        @dataclass
        class ToListObserver(Observer[U]):
            acc: list[U]

            def on_next(self, item: U):
                self.acc.append(item)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, item: U):
                self.acc.append(item)
                return args.observer.on_next_and_complete(self.acc)

            def on_completed(self):
                return args.observer.on_next_and_complete(self.acc)

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        @dataclass
        class BatchedObserver(Observer[U]):
            size: int
            acc: list[U]

            def on_next(self, item: U):
                self.acc.append(item)

                if len(self.acc) == self.size:
                    batched = self.acc
                    self.acc = []
                    return args.observer.on_next(batched)

                else:
                    return continuationmonad.from_(None)

            def on_next_and_complete(self, item: U):
                self.acc.append(item)
                return args.observer.on_next_and_complete(self.acc)

            def on_completed(self):
                return args.observer.on_next_and_complete(self.acc)

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        if self.size is None:
            observer = ToListObserver(acc=[])

        else:
            observer = BatchedObserver(acc=[], size=self.size)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=observer,
                weight=args.weight,
            ),
        )


def init_to_list_flowable[U](
    child: FlowableNode[U],
    size: int | None = None,
):
    return ToListFlowable[U](
        child=child,
        size=size,
    )
