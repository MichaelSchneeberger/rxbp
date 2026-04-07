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
class ToListObserver[U](Observer[U]):
    observer: Observer[list[U]]
    acc: list[U]

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        self.acc.append(item)
        return observer.on_success(trampoline, None)

    @override
    def on_next_and_complete(self, item: U):
        self.acc.append(item)
        return self.observer.on_next_and_complete(self.acc)

    @override
    def on_completed(self):
        return self.observer.on_next_and_complete(self.acc)

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)

@dataclass
class BatchedObserver[U](Observer[U]):
    observer: Observer[list[U]]
    size: int
    acc: list[U]

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        self.acc.append(item)

        if len(self.acc) == self.size:
            batched = self.acc
            self.acc = []
            return self.observer.on_next(trampoline, batched, observer)

        else:
            return observer.on_success(trampoline, None)

    @override
    def on_next_and_complete(self, item: U):
        self.acc.append(item)
        return self.observer.on_next_and_complete(self.acc)

    @override
    def on_completed(self):
        return self.observer.on_next_and_complete(self.acc)

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class ToListFlowable[S: State, U](SingleChildFlowableNode[S, U, U]):
    child: FlowableNode[S, U]
    size: int | None

    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[list[U]],
    ) -> tuple[S, SubscriptionResult]:

        if self.size is None:
            observer = ToListObserver(
                observer=args.observer,
                acc=[],
            )

        else:
            observer = BatchedObserver(
                observer=args.observer,
                acc=[], 
                size=self.size,
            )

        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=observer,
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_to_list_flowable[S: State, U](
    child: FlowableNode[S, U],
    size: int | None = None,
):
    return ToListFlowable[S, U](
        child=child,
        size=size,
    )
