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
    NotActiveState,
    OnCompleteState,
    OnErrorState,
    OnNextState,
    WaitFurtherItemsState,
)
from rxbp.flowabletree.operations.zip.transitions import (
    RequestTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
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
        def on_next_ackowledgment(
            _: Trampoline,
            observer: DeferredObserver
        ):
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
                case WaitFurtherItemsState(certificate=certificate):
                    return certificate

                # all upstream items received
                case OnNextState():
                    _, zipped_values = zip(*sorted(state.values.items()))
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
                        certificate = self.shared.downstream.on_next_and_complete(zipped_values)
                        return certificate

                    else:
                        _ = yield from self.shared.downstream.on_next(zipped_values)

                        certificate, *certificates = yield from continuationmonad.from_(
                            None
                        ).connect(observers)

                        self.shared.transition = RequestTransition(
                            certificates=tuple(certificates),
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
                        )

                        return continuationmonad.from_(certificate)
                
                case NotActiveState(certificate=certificate):
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
            case WaitFurtherItemsState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case OnNextState():
                _, zipped_values = zip(*sorted(state.values.items()))

                return self.shared.downstream.on_next_and_complete(zipped_values)

            case NotActiveState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_completed(self):
        transition = OnCompletedTransition(child=None)  # type: ignore

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompleteState():
                return self.shared.downstream.on_completed()

            case NotActiveState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(
        self, exception: Exception
    ):
        transition = OnErrorTransition(
            child=None,  # type: ignore
            exception=exception,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnErrorState(exception=exception):
                return self.shared.downstream.on_error(exception)

            case NotActiveState(certificate=certificate):
                return continuationmonad.from_(certificate)
            
            case _:
                raise Exception(f"Unexpected state {state}.")
