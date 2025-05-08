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
class AccumulateFlowable[U, V](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    func: Callable[[V, U], V]
    init: V

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class AccumulateObserver(Observer[U]):
            acc: V

            def on_next(self, item: U):
                self.acc = outer_self.func(self.acc, item)
                return args.observer.on_next(self.acc)

            def on_next_and_complete(self, item: U):
                self.acc = outer_self.func(self.acc, item)
                return args.observer.on_next_and_complete(self.acc)

            def on_completed(self):
                return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=AccumulateObserver(acc=self.init),
                weight=args.weight,
            ),
        )


def init_accumulate_flowable[U, V](
    child: FlowableNode[U],
    func: Callable[[V, U], V],
    init: V,
):
    return AccumulateFlowable[U, V](
        child=child,
        func=func,
        init=init,
    )
