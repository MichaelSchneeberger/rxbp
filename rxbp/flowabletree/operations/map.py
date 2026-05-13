from __future__ import annotations

from dataclasses import dataclass, field
from logging import Logger
from typing import Callable, override

from contcert.typing import ContCert
from lambdatrace import StackTrace, format_lambda_tb_from_exception
from rxbp.flowabletree.notifications import Notification, OnCompleted, OnNext, OnNextAndComplete, OnError, init_on_error

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


type Err[E] = E | Exception

@dataclass(frozen=True)
class MapObserver[U, V, E](Observer[U, E]):
    observer: Observer[V, Err[E]]
    func: Callable[[U], V]
    stacks: tuple[StackTrace, ...] = field(repr=False)
    lambda_trace_logger: Logger

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext() | OnNextAndComplete():

                try:
                    n_item: V = self.func(notification.p_item)

                except Exception as exception:
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

                return self.observer.notify(
                    notification.update_item(
                        item=n_item,
                    )
                )

            case OnCompleted() | OnError():
                return self.observer.notify(notification)


@dataclass(frozen=True)
class MapFlowable[S: State, U, V, E](SingleChildFlowableNode[S, U, V, Err[E]]):
    child: FlowableNode[S, U, E]
    func: Callable[[U], V]
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[V, Err[E]],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            observer=MapObserver[U, V, E](
                observer=observer,
                func=self.func,
                stacks=self.stacks,
                lambda_trace_logger=state.p_lambda_trace_logger,
            ),
            args=args,
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_map_flowable[S: State, U, V, E](
    child: FlowableNode[S, U, E],
    func: Callable[[U], V],
    stacks: tuple[StackTrace, ...],
):
    return MapFlowable[S, U, V, E](
        child=child,
        func=func,
        stacks=stacks,
    )
