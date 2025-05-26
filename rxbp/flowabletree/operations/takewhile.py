from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from dataclassabc import dataclassabc

import continuationmonad

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class TakeWhileFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    predicate: Callable[[U], bool]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class TakeWhileObserver(Observer):

            def on_next(self, item: U):
                if outer_self.predicate(item):
                    return args.observer.on_next(item)
                
                else:
                    def on_next_subscription(_, __):
                        return args.observer.on_completed()

                    return continuationmonad.defer(on_next_subscription)

            def on_next_and_complete(self, item: U):
                if outer_self.predicate(item):
                    return args.observer.on_next_and_complete(item)
                
                else:
                    return args.observer.on_completed()

            def on_completed(self):
                return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=TakeWhileObserver(),
            ),
        )


def init_take_while_flowable[U](
    child: FlowableNode[U],
    predicate: Callable[[U], bool],
):
    return TakeWhileFlowable[U](
        child=child,
        predicate=predicate,
    )
