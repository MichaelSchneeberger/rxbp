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
    init_on_next_and_complete,
    init_on_error,
)

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


type Err[E] = E | Exception

@dataclass
class ReduceObserver[U, E](Observer[U, E]):
    observer: Observer[U, Err[E]]
    func: Callable[[U, U], U]
    is_first: bool
    acc: U | None
    stacks: tuple[StackTrace, ...] = field(repr=False)
    lambda_trace_logger: Logger

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                try:
                    if self.is_first:
                        self.is_first = False
                        self.acc = notification.p_item
                    else:
                        assert self.acc is not None
                        self.acc = self.func(self.acc, notification.p_item)
                except Exception as exception:
                    return self._handle_error(notification, exception)

                return notification.p_observer.notify(
                    rxsingle.init_on_success(
                        item=None,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnNextAndComplete():
                try:
                    if self.is_first:
                        n_item = notification.p_item
                    else:
                        assert self.acc is not None
                        n_item = self.func(self.acc, notification.p_item)
                except Exception as exception:
                    return self._handle_error(notification, exception)

                return self.observer.notify(
                    init_on_next_and_complete(
                        item=n_item,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnCompleted():
                if self.is_first:
                    # Empty sequence
                    return self._handle_error(notification, Exception("Reduce: empty sequence"))
                
                assert self.acc is not None
                return self.observer.notify(
                    init_on_next_and_complete(
                        item=self.acc,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnError():
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
class ReduceFlowable[S: State, U, E](SingleChildFlowableNode[S, U, U, Err[E]]):
    child: FlowableNode[S, U, E]
    func: Callable[[U, U], U]
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
            observer=ReduceObserver[U, E](
                observer=observer,
                func=self.func,
                is_first=True,
                acc=None,
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


def init_reduce_flowable[S: State, U, E](
    child: FlowableNode[S, U, E],
    func: Callable[[U, U], U],
    stacks: tuple[StackTrace, ...],
):
    return ReduceFlowable[S, U, E](
        child=child,
        func=func,
        stacks=stacks,
    )
