from dataclasses import dataclass
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.typing import ContinuationCertificate, ContinuationMonad, DeferredSubscription, Scheduler

from rxbp.state import State, init_state
from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode



@dataclass
class FlatMapNestedObserver[V](Observer[V]):
    downstream: Observer
    upstream: DeferredSubscription | None

    def on_next(self, value: V) -> ContinuationMonad[None]:
        return self.downstream.on_next(value)

    @do()
    def on_next_and_complete(self, value: V):
        yield from self.downstream.on_next(value)
        return self.on_completed

    @do()
    def on_completed(self):
        match self.upstream:
            case None:
                return self.downstream.on_completed()
            case _:
                trampoline = yield from continuationmonad.get_trampoline()
                certificate = self.upstream.on_next(trampoline, None)
                return continuationmonad.from_(certificate)

    def on_error(self, exception: Exception) -> ContinuationMonad[ContinuationCertificate]:
        return self.downstream.on_error(exception)


@dataclass
class FlatMapObserver[V](Observer[V]):
    downstream: Observer
    func: Callable[[V], FlowableNode]
    scheduler: Scheduler | None

    @do()
    def _on_next(self, value: V, subscription: DeferredSubscription | None):
        observer = FlatMapNestedObserver(
            downstream=self.downstream,
            upstream=subscription,
        )
        flowable = self.func(value)

        trampoline = yield from continuationmonad.get_trampoline()

        state = init_state(
            subscription_trampoline=trampoline,
            scheduler=self.scheduler,
        )

        # Todo: Handle cancellable
        _, results = flowable.unsafe_subscribe(state, observer)

        return continuationmonad.from_(results.certificate)

    def on_next(self, value: V) -> ContinuationMonad[None]:
        def on_next_ackowledgment(subscription: DeferredSubscription):
            return self._on_next(value, subscription)

        return continuationmonad.defer(on_next_ackowledgment)

    def on_next_and_complete(self, value: V) -> ContinuationMonad[ContinuationCertificate]:
        return self._on_next(value, None)

    def on_completed(self) -> ContinuationMonad[ContinuationCertificate]:
        return self.downstream.on_completed()

    def on_error(self, exception: Exception) -> ContinuationMonad[ContinuationCertificate]:
        return self.downstream.on_error(exception)



@dataclass(frozen=True)
class FlatMap[V](SingleChildFlowableNode[V, V]):
    func: Callable[[V], FlowableNode]

    def unsafe_subscribe(
        self, state: State, observer: Observer[V],
    ) -> tuple[State, ObserveResult]:
        flat_map_observer = FlatMapObserver(
            downstream=observer,
            func=self.func,
            scheduler=state.scheduler,
        )

        return self.child.unsafe_subscribe(state, flat_map_observer)
