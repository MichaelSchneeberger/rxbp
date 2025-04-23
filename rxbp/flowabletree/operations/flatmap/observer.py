from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    DeferredObserver,
)

from rxbp.state import init_state
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.states import CancelledState
from rxbp.flowabletree.operations.flatmap.transitions import UpdateCancellableTransition
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

        # subscriber_count = {}
        # shared_weights = {}

        # flowable.discover(subscriber_count)
        # flowable.assign_weights(1, shared_weights, subscriber_count)

        # raise NotImplementedError

        trampoline = yield from continuationmonad.get_trampoline()

        # state = init_state(
        #     subscription_trampoline=trampoline,
        #     scheduler=self.scheduler,
        # )

        # observer = FlatMapNestedObserver(
        #     downstream=self.downstream,
        #     upstream=deferred_observer,
        #     shared=self.shared,
        # )

        result = flowable.subscribe(
            state=init_state(
                subscription_trampoline=trampoline,
                scheduler=self.scheduler,
            ),
            args=SubscribeArgs(
                observer=FlatMapNestedObserver(
                    downstream=self.downstream,
                    upstream=deferred_observer,
                    shared=self.shared,
                ),
                schedule_weight=self.schedule_weight,
            )
        )

        # certificate = subscrption_task()

        # certificate = trampoline.schedule(subscrption_task, weight=self.schedule_weight)

        # _, result = flowable.unsafe_subscribe(
        #     state,
        #     SubscribeArgs(
        #         observer=observer,
        #         schedule_weight=self.schedule_weight,
        #     ),
        # )

        transition = UpdateCancellableTransition(
            child=None,  # type: ignore
            cancellable=result.cancellable,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
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
