from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, override

from contcert.typing import Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode


@dataclass(frozen=True)
class MapObserver[U, V](Observer[U]):
    observer: Observer[V]
    func: Callable[[U], V]

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        return self.observer.on_next(trampoline, self.func(item), observer)

    @override
    def on_next_and_complete(self, item: U):
        return self.observer.on_next_and_complete(self.func(item))

    @override
    def on_completed(self):
        return self.observer.on_completed()

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class MapFlowable[S: State, U, V](SingleChildFlowableNode[S, U, V]):
    child: FlowableNode[S, U]
    func: Callable[[U], V]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[V],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=MapObserver[U, V](
                    observer=args.observer,
                    func=self.func,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_map_flowable[S: State, U, V](
    child: FlowableNode[S, U],
    func: Callable[[U], V],
):
    return MapFlowable[S, U, V](
        child=child,
        func=func,
    )
