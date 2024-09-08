from abc import abstractmethod
from typing import override

from donotation import do

from continuationmonad import fork, schedule_on, init_cancellable

from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.state import State


class FromValue[V](FlowableNode[V]):
    @property
    @abstractmethod
    def value(self) -> V: ...

    @override
    def unsafe_subscribe(
        self, state: State, observer: Observer[V],
    ) -> tuple[State, ObserveResult]:
        @do()
        def send_item_on_scheduler():            
            if state.scheduler:
                yield from schedule_on(state.scheduler)

            return observer.on_next_and_complete(self.value)

        cancellable = init_cancellable()

        certificate = fork(
            continuation=send_item_on_scheduler,
            scheduler=state.subscription_trampoline,
            cancellable=cancellable,
        )

        return state, ObserveResult(certificate=certificate, cancellable=cancellable)
