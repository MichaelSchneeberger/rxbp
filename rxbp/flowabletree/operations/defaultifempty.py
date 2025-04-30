from __future__ import annotations

from dataclasses import dataclass
from dataclassabc import dataclassabc

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class DefaultIfEmptyFlowable[U, V](SingleChildFlowableNode[U, U | V]):
    child: FlowableNode[U]
    value: V

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class DefaultIfEmptyObserver(Observer):
            is_empty: bool

            def on_next(self, item: U):
                self.is_empty = False
                return args.observer.on_next(item)

            def on_next_and_complete(self, item: U):
                return args.observer.on_next_and_complete(item)

            def on_completed(self):
                if self.is_empty:
                    return args.observer.on_next_and_complete(outer_self.value)
                
                else:
                    return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=DefaultIfEmptyObserver(is_empty=True),
                schedule_weight=args.schedule_weight,
            ),
        )


def init_default_if_empty_flowable[U, V](
    child: FlowableNode[U],
    value: V,
):
    return DefaultIfEmptyFlowable[U, V](
        child=child,
        value=value,
    )
