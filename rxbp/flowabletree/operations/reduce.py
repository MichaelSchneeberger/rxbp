from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, override

from dataclasses import dataclass


from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


@dataclass
class ReduceObserver[U](Observer[U]):
    observer: Observer[U]
    func: Callable[[U, U], U]
    is_first: bool
    acc: U | None

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        if self.is_first:
            self.is_first = False
            self.acc = item

        else:
            self.acc = self.func(self.acc, item)
        
        return observer.on_success(trampoline, None)

    @override
    def on_next_and_complete(self, item: U):
        match self.acc:
            case None:
                return self.observer.on_error(Exception("Not good."))
            case _:
                self.acc = self.func(self.acc, item)
                return self.observer.on_next_and_complete(self.acc)

    @override
    def on_completed(self):
        match self.acc:
            case None:
                return self.observer.on_error(Exception("Not good."))
            case _:
                return self.observer.on_next_and_complete(self.acc)

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class ReduceFlowable[S: State, U](SingleChildFlowableNode[S, U, U]):
    child: FlowableNode[S, U]
    func: Callable[[U, U], U]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=ReduceObserver(
                    observer=args.observer,
                    func=self.func,
                    is_first=True,
                    acc=None,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_reduce_flowable[S: State, U](
    child: FlowableNode[S, U],
    func: Callable[[U, U], U],
):
    return ReduceFlowable[S, U](
        child=child,
        func=func,
    )
