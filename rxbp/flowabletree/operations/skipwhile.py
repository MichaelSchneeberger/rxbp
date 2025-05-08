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
class SkipWhileFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    predicate: Callable[[U], bool]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class SkipWhileObserver(Observer):
            def on_next(self, item: U):
                if outer_self.predicate(item):
                    return continuationmonad.from_(None)

                else:
                    return args.observer.on_next(item)

            def on_next_and_complete(self, item: U):
                if outer_self.predicate(item):
                    return args.observer.on_completed()
                
                else:
                    return args.observer.on_next_and_complete(item)

            def on_completed(self):
                return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=SkipWhileObserver(),
                weight=args.weight,
            ),
        )


def init_skip_while_flowable[U](
    child: FlowableNode[U],
    predicate: Callable[[U], bool],
):
    return SkipWhileFlowable[U](
        child=child,
        predicate=predicate,
    )
