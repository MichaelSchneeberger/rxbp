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
class FilterFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    predicate: Callable[[U], bool]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class FilterObserver(Observer):
            def on_next(self, item: U):
                if outer_self.predicate(item):
                    return args.observer.on_next(item)
                else:
                    return continuationmonad.from_(None)

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
            args=SubscribeArgs(
                observer=FilterObserver(),
                schedule_weight=args.schedule_weight,
            ),
        )


def init_filter_flowable[U](
    child: FlowableNode[U],
    predicate: Callable[[U], bool],
):
    return FilterFlowable[U](
        child=child,
        predicate=predicate,
    )
