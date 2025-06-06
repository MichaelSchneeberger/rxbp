from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    DeferredHandler,
)

from rxbp.flowabletree.operations.merge.states import (
    CancelledStopRequestState,
    KeepWaitingState,
    AwaitOnNextState,
    MergeState,
    OnCompletedState,
    OnErrorState,
    OnNextAndCompleteState,
    OnNextState,
    HasTerminatedState,
    StopContinuationStateMixin,
)
from rxbp.flowabletree.operations.merge.statetransitions import (
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
    id: int

    @do()
    def _on_next(self, state: MergeState):
        match state:
            case OnNextState(
                value=value,
                observer=observer,
            ):
                _ = yield from self.shared.downstream.on_next(value)

                if observer is None:
                    certificate = None

                else:
                    certificate, *_ = yield from continuationmonad.from_(None).connect(
                        (observer,)
                    )

                transition = RequestTransition(
                    child=None,  # type: ignore
                    id=self.id,
                    certificate=certificate,
                )

                with self.shared.lock:
                    transition.child = self.shared.transition
                    state = transition.get_state()
                    self.shared.transition = ToStateTransition(state=state)

                match state:
                    case CancelledStopRequestState(
                        certificates=certificates, 
                        certificate=certificate,
                    ):
                        self.shared.cancellables[self.id].cancel(certificates[self.id])
                        return continuationmonad.from_(certificate)
                    
                    case _:
                        pass

                continuation = self._on_next(state)
                return continuation

            case KeepWaitingState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case AwaitOnNextState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case OnNextAndCompleteState(value=value):
                return self.shared.downstream.on_next_and_complete(value)

            case HasTerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    @do()
    def on_next(self, value: V):
        # print(f'on_next({value}), id={self.id}')

        # wait for upstream observer before continuing to simplify concurrency
        def on_next_subscription(_, handler: DeferredHandler):
            transition = OnNextTransition(
                child=None,  # type: ignore
                id=self.id,
                value=value,
                observer=handler,
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            state = transition.get_state()
            certificate = self._on_next(state)
            return certificate

        return continuationmonad.defer(on_next_subscription)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f'on_next_and_complete({value}), id={self.id}')

        transition = OnNextAndCompleteTransition(
            child=None,  # type: ignore
            id=self.id,
            value=value,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        state = transition.get_state()
        return self._on_next(state)

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # type: ignore
            id=self.id,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompletedState():
                return self.shared.downstream.on_completed()

            case StopContinuationStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        transition = OnErrorTransition(
            child=None,  # type: ignore
            id=self.id,
            exception=exception,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnErrorState(
                exception=exception,
                certificates=certificates,
            ):
                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel(certificate)
        
                return self.shared.downstream.on_error(exception)

            case HasTerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
