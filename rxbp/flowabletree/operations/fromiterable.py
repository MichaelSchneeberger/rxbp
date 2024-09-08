from abc import abstractmethod
from typing import Iterable, override

from donotation import do

from continuationmonad import fork, schedule_on, init_cancellable

from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.state import State


class FromIterable[V](FlowableNode[V]):
    @property
    @abstractmethod
    def iterable(self) -> Iterable[V]: ...

    @override
    @do()
    def unsafe_subscribe(
        self,
        state: State,
        observer: Observer[V],
    ) -> tuple[State, ObserveResult]:
        iterator = iter(self.iterable)

        @do()
        def schedule_and_send_next():
            try:
                next_item, has_next = next(iterator), True
            except StopIteration:
                next_item, has_next = None, False

            if has_next:
                if state.scheduler:
                    # schedule sending action on dedicated scheduler
                    yield from schedule_on(state.scheduler)

                # receive acknowledgment
                _ = yield from observer.on_next(next_item)

                return schedule_and_send_next()

            else:
                return observer.on_completed()

        cancellable = init_cancellable()

        certificate = fork(
            continuation=schedule_and_send_next,
            scheduler=state.subscription_trampoline,
        )

        return state, ObserveResult(cancellable=cancellable, certificate=certificate)
