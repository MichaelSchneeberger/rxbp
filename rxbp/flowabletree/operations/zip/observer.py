from __future__ import annotations

from dataclasses import dataclass
from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Trampoline,
    DeferredObserver,
)

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.states import (
    CancelledState,
    OnNextAndCompleteState,
    TerminatedStateMixin,
    OnCompletedState,
    OnErrorState,
    OnNextState,
    AwaitFurtherState,
)
from rxbp.flowabletree.operations.zip.statetransitions import (
    RequestTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory


@dataclass
class ZipObserver[V](Observer[V]):
    shared: ZipSharedMemory
    id: int

    def on_next(self, value: V):
        # print(f'on_next({value}), id={self.id}')

        # wait for upstream subscription before continuing to simplify concurrency
        @do()
        def on_next_ackowledgment(_: Trampoline, observer: DeferredObserver):
            # print(f'on_next_ack({value}), id={self.id}, weight={observer.weight}')

            transition = OnNextTransition(
                id=self.id,
                value=value,
                observer=observer,
                n_children=self.shared.n_children,
                child=None,  # type: ignore
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            match state := transition.get_state():
                # wait for other upstream items
                case AwaitFurtherState(certificate=certificate):
                    return certificate

                # all upstream items received
                case OnNextState():
                    _, zipped_values = zip(*sorted(state.values.items()))

                    # backpressure selected upstream flowables
                    hold_back = self.shared.zip_func(state.values)

                    complete_downstream = [False]

                    def gen_deferred_observers():
                        for id in state.values:
                            if id not in hold_back:
                                if id in state.observers:
                                    yield state.observers[id]
                                else:
                                    complete_downstream[0] = True

                    observers = tuple(gen_deferred_observers())

                    if complete_downstream[0]:
                        certificate = self.shared.downstream.on_next_and_complete(
                            zipped_values
                        )
                        return certificate

                    else:
                        _ = yield from self.shared.downstream.on_next(zipped_values)

                        certificate, *others = yield from continuationmonad.from_(
                            None
                        ).connect(observers)

                        transition = RequestTransition(
                            child=None,  # type: ignore
                            certificates=tuple(others),
                            values={
                                id: value
                                for id, value in state.values.items()
                                if id in hold_back
                            },
                            observers={
                                id: value
                                for id, value in state.observers.items()
                                if id in hold_back
                            },
                            n_children=self.shared.n_children,
                        )

                        with self.shared.lock:
                            transition.child = self.shared.transition
                            n_state = transition.get_state()
                            self.shared.transition = ToStateTransition(
                                state=n_state
                            )

                        match n_state:
                            case CancelledState(certificates=certificates):
                                def cancel_upstream():
                                    for id, certificate in certificates.items():
                                        self.shared.cancellables[id].cancel(certificate)
                                cancel_upstream()

                        return continuationmonad.from_(certificate)

                case TerminatedStateMixin(certificate=certificate):
                    return certificate

                case _:
                    raise Exception(f"Unexpected state {state}.")

        return continuationmonad.defer(on_next_ackowledgment)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f'on_next_and_complete({value}), id={self.id}')

        transition = OnNextAndCompleteTransition(
            id=self.id,
            value=value,
            n_children=self.shared.n_children,
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            # wait for other upstream items
            case AwaitFurtherState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case OnNextAndCompleteState():
                _, zipped_values = zip(*sorted(state.values.items()))

                return self.shared.downstream.on_next_and_complete(zipped_values)

            case TerminatedStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,
            n_children=self.shared.n_children,
            id=self.id,
        )  # type: ignore

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompletedState(
                certificates=certificates,
            ):
                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel(certificate)

                return self.shared.downstream.on_completed()

            case TerminatedStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        transition = OnErrorTransition(
            child=None,  # type: ignore
            n_children=self.shared.n_children,
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

            case TerminatedStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
