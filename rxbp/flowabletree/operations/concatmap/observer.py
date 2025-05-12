from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    DeferredHandler,
)

from rxbp.flowabletree.subscribeandconnect import subscribe_single_sink
from rxbp.state import init_state
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.concatmap.sharedmemory import ConcatMapSharedMemory
from rxbp.flowabletree.operations.concatmap.states import CancelledState
from rxbp.flowabletree.operations.concatmap.statetransitions import UpdateCancellableTransition
from rxbp.flowabletree.operations.concatmap.innerobserver import ConcatMapInnerObserver


@dataclass
class ConcatMapObserver[U](Observer[U]):
    downstream: Observer
    func: Callable[[U], FlowableNode[U]]
    scheduler: Scheduler
    shared: ConcatMapSharedMemory
    weight: int

    @do()
    def _on_next(self, item: U, handler: DeferredHandler | None):
        # print('apply func')

        flowable = self.func(item)

        trampoline = yield from continuationmonad.get_trampoline()

        try:
            state = init_state(
                subscription_trampoline=trampoline,
                scheduler=self.scheduler,
            )

            sink = ConcatMapInnerObserver(
                downstream=self.downstream,
                upstream=handler,
                shared=self.shared,
            )

            state, result = subscribe_single_sink(
                source=flowable,
                sink=sink,
                state=state,
                weight=self.weight,
            )

            # result = flowable.subscribe(
            #     state=init_state(
            #         subscription_trampoline=trampoline,
            #         scheduler=self.scheduler,
            #     ),
            #     args=SubscribeArgs(
            #         observer=ConcatMapInnerObserver(
            #             downstream=self.downstream,
            #             upstream=handler,
            #             shared=self.shared,
            #         ),
            #         schedule_weight=self.schedule_weight,
            #     )
            # )

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

    def on_next(self, item: U):
        def on_next_subscription(_, handler: DeferredHandler):
            return self._on_next(item, handler)

        return continuationmonad.defer(on_next_subscription)

    def on_next_and_complete(self, item: U):
        return self._on_next(item, None)

    def on_completed(self):
        return self.downstream.on_completed()

    def on_error(
        self, exception: Exception
    ):
        return self.downstream.on_error(exception)
