from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    DeferredObserver,
)

from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.state import init_state
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.states import CancelledState
from rxbp.flowabletree.operations.flatmap.actions import UpdateCancellableAction
from rxbp.flowabletree.operations.flatmap.innerobserver import FlatMapNestedObserver


@dataclass
class FlatMapObserver[V](Observer[V]):
    downstream: Observer
    func: Callable[[V], FlowableNode]
    scheduler: Scheduler | None
    shared: FlatMapSharedMemory
    schedule_weight: int

    @do()
    def _on_next(self, value: V, deferred_observer: DeferredObserver | None):
        flowable = self.func(value)

        trampoline = yield from continuationmonad.get_trampoline()

        state = init_state(
            subscription_trampoline=trampoline,
            scheduler=self.scheduler,
        )

        observer = FlatMapNestedObserver(
            downstream=self.downstream,
            upstream=deferred_observer,
            shared=self.shared,
        )

        _, result = flowable.unsafe_subscribe(
            state,
            SubscribeArgs(
                observer=observer,
                schedule_weight=self.schedule_weight,
            ),
        )

        action = UpdateCancellableAction(
            child=None,  # type: ignore
            cancellable=result.cancellable,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match action.get_state():
            case CancelledState(certificate=certificate, cancellable=cancellable):
                cancellable.cancel(certificate)

        return continuationmonad.from_(result.certificate)

    def on_next(self, value: V):
        def on_next_ackowledgment(_, subscription: DeferredObserver):
            return self._on_next(value, subscription)

        return continuationmonad.defer(on_next_ackowledgment)

    def on_next_and_complete(
        self, value: V
    ):
        return self._on_next(value, None)

    def on_completed(self):
        return self.downstream.on_completed()

    def on_error(
        self, exception: Exception
    ):
        return self.downstream.on_error(exception)
