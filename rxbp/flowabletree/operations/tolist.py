from __future__ import annotations

from dataclasses import dataclass
from typing import override

import rxsingle
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
class ToListObserver[U, E](Observer[U, E]):
    observer: Observer[list[U], E]
    acc: list[U]

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                self.acc.append(notification.p_item)
                return notification.p_observer.notify(
                    rxsingle.init_on_success(
                        item=None,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnNextAndComplete():
                self.acc.append(notification.p_item)
                return self.observer.notify(
                    init_on_next_and_complete(
                        item=self.acc,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnCompleted():
                return self.observer.notify(
                    init_on_next_and_complete(
                        item=self.acc,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnError():
                return self.observer.notify(notification)

@dataclass
class BatchedObserver[U, E](Observer[U, E]):
    observer: Observer[list[U], E]
    size: int
    acc: list[U]

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                self.acc.append(notification.p_item)

                if len(self.acc) == self.size:
                    batched = self.acc
                    self.acc = []
                    return self.observer.notify(
                        notification.update_item(
                            item=batched,
                        )
                    )

                else:
                    return notification.p_observer.notify(
                        rxsingle.init_on_success(
                            item=None,
                            trampoline=notification.p_trampoline,
                            item_scheduler=notification.p_item_scheduler,
                        )
                    )

            case OnNextAndComplete():
                self.acc.append(notification.p_item)
                return self.observer.notify(
                    init_on_next_and_complete(
                        item=self.acc,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )

            case OnCompleted():
                if self.acc:
                    return self.observer.notify(
                        init_on_next_and_complete(
                            item=self.acc,
                            trampoline=notification.p_trampoline,
                            item_scheduler=notification.p_item_scheduler,
                        )
                    )
                else:
                    return self.observer.notify(notification)

            case OnError():
                return self.observer.notify(notification)


@dataclass(frozen=True)
class ToListFlowable[S: State, U, E](SingleChildFlowableNode[S, U, list[U], E]):
    child: FlowableNode[S, U, E]
    size: int | None

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[list[U], E],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:

        if self.size is None:
            n_observer = ToListObserver(
                observer=observer,
                acc=[],
            )

        else:
            n_observer = BatchedObserver(
                observer=observer,
                acc=[], 
                size=self.size,
            )

        return self.child.unsafe_subscribe(
            state=state,
            observer=n_observer,
            args=args,
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self) -> FlowableNode[S, U, E]:
        return self.child


def init_to_list_flowable[S: State, U, E](
    child: FlowableNode[S, U, E],
    size: int | None = None,
):
    return ToListFlowable[S, U, E](
        child=child,
        size=size,
    )
