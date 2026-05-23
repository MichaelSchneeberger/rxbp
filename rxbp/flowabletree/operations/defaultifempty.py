from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert
from rxbp.flowabletree.notifications import (
    Notification,
    OnNext,
    OnNextAndComplete,
    OnCompleted,
    OnError,
    init_on_next_and_complete,
)

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


@dataclass
class DefaultIfEmptyObserver[U, E](Observer[U, E]):
    observer: Observer[U, E]
    value: U
    is_empty: bool

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                self.is_empty = False
                return self.observer.notify(notification)

            case OnNextAndComplete():
                self.is_empty = False
                return self.observer.notify(notification)

            case OnCompleted():
                if self.is_empty:
                    return self.observer.notify(
                        init_on_next_and_complete(
                            item=self.value,
                            trampoline=notification.p_trampoline,
                            item_scheduler=notification.p_item_scheduler,
                        )
                    )
                else:
                    return self.observer.notify(notification)

            case OnError():
                return self.observer.notify(notification)


@dataclass(frozen=True)
class DefaultIfEmptyFlowable[S: State, U, E](SingleChildFlowableNode[S, U, U, E]):
    child: FlowableNode[S, U, E]
    value: U

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            observer=DefaultIfEmptyObserver[U, E](
                observer=observer,
                value=self.value,
                is_empty=True,
            ),
            args=args,
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self) -> FlowableNode[S, U, E]:
        return self.child


def init_default_if_empty_flowable[S: State, U, E](
    child: FlowableNode[S, U, E],
    value: U,
):
    return DefaultIfEmptyFlowable[S, U, E](
        child=child,
        value=value,
    )
