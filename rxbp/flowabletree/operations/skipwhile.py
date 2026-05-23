from __future__ import annotations

from dataclasses import dataclass, field
from logging import Logger
from typing import Callable, override

from contcert.typing import ContCert
import rxsingle
from lambdatrace import StackTrace, format_lambda_tb_from_exception
from rxbp.flowabletree.notifications import (
    Notification,
    OnNext,
    OnNextAndComplete,
    OnCompleted,
    OnError,
    init_on_completed,
    init_on_error,
)

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


type Err[E] = E | Exception

@dataclass
class SkipWhileObserver[U, E](Observer[U, E]):
    observer: Observer[U, Err[E]]
    predicate: Callable[[U], bool]
    is_skipping: bool
    stacks: tuple[StackTrace, ...] = field(repr=False)
    lambda_trace_logger: Logger

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                if self.is_skipping:
                    try:
                        if self.predicate(notification.p_item):
                            return notification.p_observer.notify(
                                rxsingle.init_on_success(
                                    item=None,
                                    trampoline=notification.p_trampoline,
                                    item_scheduler=notification.p_item_scheduler,
                                )
                            )
                        else:
                            self.is_skipping = False
                    except Exception as exception:
                        return self._handle_error(notification, exception)

                return self.observer.notify(notification)

            case OnNextAndComplete():
                if self.is_skipping:
                    try:
                        if self.predicate(notification.p_item):
                            return self.observer.notify(
                                init_on_completed(
                                    trampoline=notification.p_trampoline,
                                    item_scheduler=notification.p_item_scheduler,
                                )
                            )
                        else:
                            self.is_skipping = False
                    except Exception as exception:
                        return self._handle_error(notification, exception)

                return self.observer.notify(notification)

            case OnCompleted() | OnError():
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
class SkipWhileFlowable[S: State, U, E](SingleChildFlowableNode[S, U, U, Err[E]]):
    child: FlowableNode[S, U, E]
    predicate: Callable[[U], bool]
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, Err[E]],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            observer=SkipWhileObserver[U, E](
                observer=observer,
                predicate=self.predicate,
                is_skipping=True,
                stacks=self.stacks,
                lambda_trace_logger=state.p_lambda_trace_logger,
            ),
            args=args,
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self) -> FlowableNode[S, U, E]:
        return self.child


def init_skip_while_flowable[S: State, U, E](
    child: FlowableNode[S, U, E],
    predicate: Callable[[U], bool],
    stacks: tuple[StackTrace, ...],
):
    return SkipWhileFlowable[S, U, E](
        child=child,
        predicate=predicate,
        stacks=stacks,
    )
