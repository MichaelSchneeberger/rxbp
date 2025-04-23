from __future__ import annotations

from dataclasses import dataclass
from donotation import do

import continuationmonad
from continuationmonad.typing import (
    DeferredObserver,
)

from rxbp.flowabletree.operations.merge.states import (
    AwaitAckState,
    AwaitNextState,
    CancelledState,
    CompleteState,
    ErrorState,
    MergeState,
    OnNextAndCompleteState,
    OnNextState,
    OnNextNoAckState,
    TerminatedState,
    UpstreamID,
)
from rxbp.flowabletree.operations.merge.transitions import (
    RequestTransition,
    ToStateTransition,
    OnCompletedTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
)
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.observer import Observer


@dataclass
class MergeObserver[V](Observer[V]):
    shared: MergeSharedMemory
    id: UpstreamID

    @do()
    def _on_next(self, state: MergeState):
        # print(state.__class__.__name__)

        match state:
            case AwaitNextState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case AwaitAckState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case OnNextAndCompleteState(value=value):
                return self.shared.downstream.on_next_and_complete(value)

            case OnNextState(value=value, observer=observer):
                _ = yield from self.shared.downstream.on_next(value)
                
                certificate, *_ = yield from continuationmonad.from_(None).connect(
                    observers=(observer,)
                )

                transition = RequestTransition(
                    child=None,  # type: ignore
                    id=self.id,
                    certificate=certificate,
                    n_children=self.shared.n_children,
                )

                with self.shared.lock:
                    transition.child = self.shared.transition
                    state = transition.get_state()
                    self.shared.transition = ToStateTransition(state=state)

                return self._on_next(state)

            case OnNextNoAckState(value=value, certificate=certificate):
                _ = yield from self.shared.downstream.on_next(value)

                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    @do()
    def on_next(self, value: V):
        # wait for upstream observer before continuing to simplify concurrency
        def on_next_ackowledgment(_, observer: DeferredObserver):
            transition = OnNextTransition(
                child=None,  # type: ignore
                id=self.id,
                value=value,
                observer=observer,
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            # print(f'on_next({value}), id={self.id}')

            return self._on_next(transition.get_state())

        return continuationmonad.defer(on_next_ackowledgment)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f'on_next_and_complete({value}), id={self.id}')
        transition = OnNextAndCompleteTransition(
            child=None,  # type: ignore
            id=self.id,
            value=value,
            n_children=self.shared.n_children,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        return self._on_next(transition.get_state())

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # type: ignore
            n_children=self.shared.n_children,
            id=self.id,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case CompleteState():
                return self.shared.downstream.on_completed()

            case (
                AwaitNextState(certificate=certificate)
                | AwaitAckState(certificate=certificate)
            ):
                return continuationmonad.from_(certificate)
                # return certificate

            case TerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)
                # return certificate

            case CancelledState(certificates=certificates):
                return continuationmonad.from_(certificates[self.id])
                # return certificates[self.id]

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(
        self, exception: Exception
    ):
        transition = OnErrorTransition(
            child=None,  # type: ignore
            id=self.id,
            n_children=self.shared.n_children,
            exception=exception,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case ErrorState(exception=exception):
                return self.shared.downstream.on_error(exception)

            case TerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)
                # return certificate
            
            case CancelledState(certificates=certificates):
                return continuationmonad.from_(certificates[self.id])
                # return certificates[self.id]

            case _:
                raise Exception(f"Unexpected state {state}.")

