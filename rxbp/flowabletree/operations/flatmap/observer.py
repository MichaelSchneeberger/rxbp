from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    DeferredHandler,
)

from rxbp.state import init_state
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.states import CancelledState
from rxbp.flowabletree.operations.flatmap.statetransitions import UpdateCancellableTransition
from rxbp.flowabletree.operations.flatmap.innerobserver import FlatMapNestedObserver


@dataclass
class FlatMapObserver[V](Observer[V]):
    downstream: Observer
    func: Callable[[V], FlowableNode[V]]
    scheduler: Scheduler | None
    shared: FlatMapSharedMemory
    schedule_weight: int

    @do()
    def _on_next(self, value: V, handler: DeferredHandler | None):
        # print('apply func')

        flowable = self.func(value)

        trampoline = yield from continuationmonad.get_trampoline()

        try:
            result = flowable.subscribe(
                state=init_state(
                    subscription_trampoline=trampoline,
                    scheduler=self.scheduler,
                ),
                args=SubscribeArgs(
                    observer=FlatMapNestedObserver(
                        downstream=self.downstream,
                        upstream=handler,
                        shared=self.shared,
                    ),
                    schedule_weight=self.schedule_weight,
                )
            )

        except Exception as exception:
            return self.downstream.on_error(exception)

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
        # print('on_next')
        def on_next_subscription(_, handler: DeferredHandler):
            # print('on_next_subscription')
            return self._on_next(value, handler)

        return continuationmonad.defer(on_next_subscription)

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
