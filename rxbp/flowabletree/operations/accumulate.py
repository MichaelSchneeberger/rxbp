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
class AccumulateObserver[U, V](Observer[U]):
    observer: Observer[V]
    func: Callable[[V, U], V]
    acc: V

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        self.acc = self.func(self.acc, item)
        return self.observer.on_next(trampoline, self.acc, observer)

    @override
    def on_next_and_complete(self, item: U):
        self.acc = self.func(self.acc, item)
        return self.observer.on_next_and_complete(self.acc)

    @override
    def on_completed(self):
        return self.observer.on_completed()

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class AccumulateFlowable[S: State, U, V](SingleChildFlowableNode[S, U, V]):
    child: FlowableNode[S, U]
    func: Callable[[V, U], V]
    init: V

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[V],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=AccumulateObserver[U, V](
                    observer=args.observer,
                    func=self.func,
                    acc=self.init,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_accumulate_flowable[S: State, U, V](
    child: FlowableNode[S, U],
    func: Callable[[V, U], V],
    init: V,
):
    return AccumulateFlowable[S, U, V](
        child=child,
        func=func,
        init=init,
    )
