from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    ContinuationMonad,
    DeferredObserver,
    ContinuationCertificate,
)

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.states import ActiveState, CancelledState
from rxbp.flowabletree.operations.flatmap.actions import (
    FromStateAction,
    UpdateCancellableAction,
)
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory


@dataclass
class FlatMapNestedObserver[V](Observer[V]):
    downstream: Observer
    upstream: DeferredObserver | None
    shared: FlatMapSharedMemory

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
        action = UpdateCancellableAction(
            child=None,  # type: ignore
            cancellable=self.shared.upstream_cancellable,
        )

        with self.shared.lock:
            action.child = self.shared.action
            state = action.get_state()
            self.shared.action = FromStateAction(state=state)

        match state:
            case ActiveState():
                match self.upstream:
                    case None:
                        return self.downstream.on_completed()

                    case DeferredObserver():
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
