from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import rxsingle
from rxsingle.typing import (
    DeferredHandler,
    RxSingle,
    ContCert,
    Trampoline,
)

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.states import (
    CancelledStopRequestState,
    KeepWaitingState,
    AwaitOnNextState,
    FlatMapState,
    OnCompletedState,
    OnErrorState,
    OnNextAndCompleteState,
    OnNextState,
    HasTerminatedState,
    StopContinuationStateMixin,
)
from rxbp.flowabletree.operations.flatmap.statetransitions import (
    RequestTransition,
    ToStateTransition,
    OnCompletedTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
)
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory


@dataclass
class FlatMapInnerObserver[V](Observer[V]):
    shared: FlatMapSharedMemory
    id: int

    @do()
    def _on_next(self, state: FlatMapState):
        match state:
            case OnNextState(
                value=value,
                observer=observer,
            ):
                _ = yield from self.shared.downstream.on_next(value)

                if observer is None:
                    certificate = None

                else:
                    certificate, *_ = yield from rxsingle.from_(None).connect(
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
                        
                        return rxsingle.from_(certificate)
                    
                    case _:
                        pass

                # help pyright to infer correct type
                continuation: RxSingle[ContCert] = self._on_next(state)

                return continuation

            case KeepWaitingState(certificate=certificate):
                return rxsingle.from_(certificate)

            case AwaitOnNextState(certificate=certificate):
                return rxsingle.from_(certificate)

            case OnNextAndCompleteState(value=value):
                return self.shared.downstream.on_next_and_complete(value)

            case HasTerminatedState(certificate=certificate):
                return rxsingle.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    @do()
    def on_next(self, value: V):
        # print(f'on_next({value}), id={self.id}')

        # wait for upstream observer before continuing to simplify concurrency
        def on_next_subscription(_: Trampoline, handler: DeferredHandler):
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

        return rxsingle.defer(on_next_subscription)

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
                return rxsingle.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        # print(f'on_error({exception}), id={self.id}')

        transition = OnErrorTransition(
            id=self.id,
            child=None,  # type: ignore
            exception=exception,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnErrorState(
                exception=exception,
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel(certificate)

                self.shared.upstream_cancellable.cancel(outer_certificate)
        
                return self.shared.downstream.on_error(exception)

            case HasTerminatedState(certificate=certificate):
                return rxsingle.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
