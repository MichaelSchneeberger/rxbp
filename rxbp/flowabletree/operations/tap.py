from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, override

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


@dataclass
class TapObserver[U](Observer[U]):
    observer: Observer[U]
    on_next_func: Callable[[U], None] | None
    on_next_and_complete_func: Callable[[U], None] | None
    on_completed_func: Callable[[], None] | None
    on_error_func: Callable[[Exception], None] | None

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
        if self.on_next_func:
            self.on_next_func(item)

        return self.observer.on_next(trampoline, item, observer)

    @override
    def on_next_and_complete(self, item: U):
        if self.on_next_and_complete_func:
            self.on_next_and_complete_func(item)

        else:
            if self.on_next_func:
                self.on_next_func(item)

            if self.on_completed_func:
                self.on_completed_func()

        return self.observer.on_next_and_complete(item)

    @override
    def on_completed(self):
        if self.on_completed_func:
            self.on_completed_func()

        return self.observer.on_completed()

    @override
    def on_error(self, exception: Exception):
        if self.on_error_func:
            self.on_error_func(exception)

        return self.observer.on_error(exception)


@dataclass(frozen=True)
class TapFlowable[S: State, U](SingleChildFlowableNode[S, U, U]):
    child: FlowableNode[S, U]
    on_next: Callable[[U], None] | None
    on_next_and_complete: Callable[[U], None] | None
    on_completed: Callable[[], None] | None
    on_error: Callable[[Exception], None] | None

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=TapObserver(
                    observer=args.observer,
                    on_next_func=self.on_next,
                    on_next_and_complete_func=self.on_next_and_complete,
                    on_completed_func=self.on_completed,
                    on_error_func=self.on_error,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_tap_flowable[S: State, U](
    child: FlowableNode[S, U],
    on_next: Callable[[U], None] | None = None,
    on_next_and_complete: Callable[[U], None] | None = None,
    on_completed: Callable[[], None] | None = None,
    on_error: Callable[[Exception], None] | None = None,
) -> TapFlowable[S, U]:
    return TapFlowable(
        child=child,
        on_next=on_next,
        on_next_and_complete=on_next_and_complete,
        on_completed=on_completed,
        on_error=on_error,
    )
