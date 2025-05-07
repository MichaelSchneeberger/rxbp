from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    ContinuationMonad,
    DeferredHandler,
    ContinuationCertificate,
)

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.concatmap.states import ActiveState, CancelledState
from rxbp.flowabletree.operations.concatmap.statetransitions import (
    ToStateTransition,
    UpdateCancellableTransition,
)
from rxbp.flowabletree.operations.concatmap.sharedmemory import ConcatMapSharedMemory


@dataclass
class ConcatMapInnerObserver[V](Observer[V]):
    downstream: Observer
    upstream: DeferredHandler | None
    shared: ConcatMapSharedMemory

    def on_next(self, value: V) -> ContinuationMonad[None]:
        return self.downstream.on_next(value)

    @do()
    def on_next_and_complete(self, value: V):
        yield from self.downstream.on_next(value)
        match result := self.on_completed():
            case ContinuationCertificate():
                return continuationmonad.from_(result)
            case _:
                return result

    @do()
    def on_completed(self):
        transition = UpdateCancellableTransition(
            child=None,  # type: ignore
            cancellable=self.shared.upstream_cancellable,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            state = transition.get_state()
            self.shared.transition = ToStateTransition(state=state)

        match state:
            case ActiveState():
                match self.upstream:
                    case None:
                        return self.downstream.on_completed()

                    case DeferredHandler():
                        return (
                            continuationmonad.from_(None)
                            .connect((self.upstream,))
                            .map(lambda c: c[0])
                        )

            case CancelledState(certificate=certificate):
                return continuationmonad.from_(certificate)
                # return certificate

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        return self.downstream.on_error(exception)
