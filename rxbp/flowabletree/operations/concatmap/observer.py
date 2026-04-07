from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from donotation import do

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.subscribe import subscribe_single_sink
from rxbp.flowabletree.subscribeargs import init_subscribe_args
from rxbp.state import init_state
from rxbp.flowabletree.flowablenode import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.concatmap.sharedmemory import ConcatMapSharedMemory
from rxbp.flowabletree.operations.concatmap.states import CancelledState
from rxbp.flowabletree.operations.concatmap.statetransitions import UpdateCancellableTransition
from rxbp.flowabletree.operations.concatmap.innerobserver import ConcatMapInnerObserver


@dataclass
class ConcatMapObserver[U](Observer[U]):
    downstream: Observer
    func: Callable[[U], FlowableNode[S, U]]
    scheduler: Scheduler
    shared: ConcatMapSharedMemory
    virtual_count: int

    @do()
    def _on_next(self, item: U, handler: DeferredHandler | None):
        # print('apply func')

        flowable = self.func(item)

        trampoline = yield from rxsingle.get_trampoline()

        try:
            state = init_state(
                subscription_trampoline=trampoline,
            )

            sink = ConcatMapInnerObserver(
                downstream=self.downstream,
                upstream=handler,
                shared=self.shared,
            )

            state, result = subscribe_single_sink(
                source=flowable,
                args=init_subscribe_args(
                    observer=sink,
                    virtual_count=self.virtual_count,
                    scheduler=self.scheduler,
                ),
                state=state,
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

        return rxsingle.from_(result.certificate)

    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]):
        def on_next_subscription(_, handler: DeferredHandler):
            return self._on_next(item, handler)

        return rxsingle.defer(on_next_subscription)

    def on_next_and_complete(self, item: U):
        return self._on_next(item, None)

    def on_completed(self):
        return self.downstream.on_completed()

    def on_error(
        self, exception: Exception
    ):
        return self.downstream.on_error(exception)
