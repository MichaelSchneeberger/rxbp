from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate, ContinuationMonad

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class DoActionFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]
    on_next: Callable[[U], None] | None
    on_next_and_complete: Callable[[U], None] | None
    on_completed: Callable[[], None] | None
    on_error: Callable[[Exception], None] | None

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        outer_self = self

        @dataclass
        class DoActionObserver(Observer):
            def on_next(self, item: U):
                if outer_self.on_next:
                    outer_self.on_next(item)

                return args.observer.on_next(item)

            def on_next_and_complete(self, item: U):
                if outer_self.on_next_and_complete:
                    outer_self.on_next_and_complete(item)

                else:
                    if outer_self.on_next:
                        outer_self.on_next(item)

                    if outer_self.on_completed:
                        outer_self.on_completed()

                return args.observer.on_next_and_complete(item)

            def on_completed(self) -> ContinuationMonad[ContinuationCertificate]:
                if outer_self.on_completed:
                    outer_self.on_completed()

                return args.observer.on_completed()

            def on_error(self, exception: Exception) -> ContinuationMonad[ContinuationCertificate]:
                if outer_self.on_error:
                    outer_self.on_error(exception)

                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=DoActionObserver(),
                schedule_weight=args.schedule_weight,
            ),
        )


def init_do_action_flowable[U](
    child: FlowableNode[U],
    on_next: Callable[[U], None] | None = None,
    on_next_and_complete: Callable[[U], None] | None = None,
    on_completed: Callable[[], None] | None = None,
    on_error: Callable[[Exception], None] | None = None,
):
    return DoActionFlowable[U](
        child=child,
        on_next=on_next,
        on_next_and_complete=on_next_and_complete,
        on_completed=on_completed,
        on_error=on_error,
    )
