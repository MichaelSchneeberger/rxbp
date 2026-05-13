from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, override

from contcert.typing import Trampoline, ContCert
import rxsingle
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode, SingleChildFlowableNode



@dataclass(frozen=True)
class FilterObserver[U](Observer[U]):
    observer: Observer[U]
    predicate: Callable[[U], bool]

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
        if self.predicate(item):
            return self.observer.on_next(trampoline, item, observer)
        else:
            return observer.on_success(trampoline, None)

    @override
    def on_next_and_complete(self, item: U):
        if self.predicate(item):
            return self.observer.on_next_and_complete(item)
        else:
            return self.observer.on_completed()

    @override
    def on_completed(self):
        return self.observer.on_completed()

    @override
    def on_error(self, exception: Exception):
        return self.observer.on_error(exception)

@dataclass(frozen=True)
class FilterFlowable[S, U](SingleChildFlowableNode[S, U, U]):
    child: FlowableNode[S, U]
    predicate: Callable[[U], bool]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        return self.child.unsafe_subscribe(
            state=state,
            args=args.copy(
                observer=FilterObserver(
                    observer=args.observer,
                    predicate=self.predicate,
                ),
            ),
        )

    """ Implement abstract methods from SingleChildFlowableNode """

    @property
    @override
    def p_child(self):
        return self.child


def init_filter_flowable[S, U](
    child: FlowableNode[S, U],
    predicate: Callable[[U], bool],
):
    return FilterFlowable[S, U](
        child=child,
        predicate=predicate,
    )
