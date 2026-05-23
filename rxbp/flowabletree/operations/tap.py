from __future__ import annotations

from dataclasses import dataclass, field
from logging import Logger
from typing import Callable, override

from contcert.typing import ContCert
from lambdatrace import StackTrace, format_lambda_tb_from_exception
from rxbp.flowabletree.notifications import (
    Notification,
    OnNext,
    OnNextAndComplete,
    OnCompleted,
    OnError,
    init_on_error,
)

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


type Err[E] = E | Exception

@dataclass
class TapObserver[U, E](Observer[U, E]):
    observer: Observer[U, Err[E]]
    on_next_func: Callable[[U], None] | None
    on_next_and_complete_func: Callable[[U], None] | None
    on_completed_func: Callable[[], None] | None
    on_error_func: Callable[[E], None] | None
    stacks: tuple[StackTrace, ...] = field(repr=False)
    lambda_trace_logger: Logger

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                if self.on_next_func:
                    try:
                        self.on_next_func(notification.p_item)
                    except Exception as exception:
                        return self._handle_error(notification, exception)

                return self.observer.notify(notification)

            case OnNextAndComplete():
                if self.on_next_and_complete_func:
                    try:
                        self.on_next_and_complete_func(notification.p_item)
                    except Exception as exception:
                        return self._handle_error(notification, exception)
                else:
                    if self.on_next_func:
                        try:
                            self.on_next_func(notification.p_item)
                        except Exception as exception:
                            return self._handle_error(notification, exception)

                    if self.on_completed_func:
                        try:
                            self.on_completed_func()
                        except Exception as exception:
                            return self._handle_error(notification, exception)

                return self.observer.notify(notification)

            case OnCompleted():
                if self.on_completed_func:
                    try:
                        self.on_completed_func()
                    except Exception as exception:
                        return self._handle_error(notification, exception)

                return self.observer.notify(notification)

            case OnError():
                if self.on_error_func:
                    try:
                        self.on_error_func(notification.p_exception)
                    except Exception as exception:
                        tb = format_lambda_tb_from_exception(
                            stacks=self.stacks,
                            exception=exception,
                        )
                        self.lambda_trace_logger.error(f"Error in tap.on_error: {tb}")

                return self.observer.notify(notification)

    def _handle_error(self, notification: Notification[U, E], exception: Exception) -> ContCert:
        tb = format_lambda_tb_from_exception(
            stacks=self.stacks,
            exception=exception,
        )
        self.lambda_trace_logger.error(tb)

        n_notification = init_on_error(
            exception=exception,
            trampoline=notification.p_trampoline,
            item_scheduler=notification.p_item_scheduler,
        )

        return self.observer.notify(n_notification)


@dataclass(frozen=True)
class TapFlowable[S: State, A, U, E](SingleChildFlowableNode[S, A, U, U, Err[E]]):
    child: FlowableNode[S, A, U, E]
    on_next: Callable[[U], None] | None
    on_next_and_complete: Callable[[U], None] | None
    on_completed: Callable[[], None] | None
    on_error: Callable[[E], None] | None
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, Err[E]],
        args: A,
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            observer=TapObserver[U, E](
                observer=observer,
                on_next_func=self.on_next,
                on_next_and_complete_func=self.on_next_and_complete,
                on_completed_func=self.on_completed,
                on_error_func=self.on_error,
                stacks=self.stacks,
                lambda_trace_logger=state.p_lambda_trace_logger,
            ),
            args=args,
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self) -> FlowableNode[S, A, U, E]:
        return self.child


def init_tap_flowable[S: State, A, U, E](
    child: FlowableNode[S, A, U, E],
    on_next: Callable[[U], None] | None = None,
    on_next_and_complete: Callable[[U], None] | None = None,
    on_completed: Callable[[], None] | None = None,
    on_error: Callable[[E], None] | None = None,
    stacks: tuple[StackTrace, ...] = tuple(),
) -> TapFlowable[S, A, U, E]:
    return TapFlowable(
        child=child,
        on_next=on_next,
        on_next_and_complete=on_next_and_complete,
        on_completed=on_completed,
        on_error=on_error,
        stacks=stacks,
    )
