from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


@dataclass
class DefaultIfEmptyObserver[U](Observer[U]):
    observer: Observer[U]
    value: U
    is_empty: bool

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        self.is_empty = False
        return self.observer.on_next(trampoline, item, observer)

    @override
    def on_next_and_complete(self, item: U):
        return self.observer.on_next_and_complete(item)

    @override
    def on_completed(self):
        if self.is_empty:
            return self.observer.on_next_and_complete(self.value)
        
        else:
            return self.observer.on_completed()

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class DefaultIfEmptyFlowable[S: State, U](SingleChildFlowableNode[S, U, U]):
    child: FlowableNode[S, U]
    value: U

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=DefaultIfEmptyObserver[U](
                    observer=args.observer,
                    value=self.value,
                    is_empty=True,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_default_if_empty_flowable[S: State, U](
    child: FlowableNode[S, U],
    value: U,
):
    return DefaultIfEmptyFlowable[S, U](
        child=child,
        value=value,
    )

