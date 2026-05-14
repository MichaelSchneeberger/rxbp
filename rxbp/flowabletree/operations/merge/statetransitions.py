from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import override


from rxbp.flowabletree.operations.merge.states import (
    ActiveState,
    CompletedState,
    ErrorState,
    StopContinuationState,
    MergeState,
)


class MergeStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> MergeState: ...


@dataclass(frozen=True)
class ToStateTransition(MergeStateTransition):
    """Transitions to predefined state"""

    state: MergeState

    @override
    def get_state(self):
        return self.state


@dataclass
class OnNextTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(self) -> ActiveState | StopContinuationState | ErrorState:
        match state := self.child.get_state():
            case ActiveState(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_received_ids = received_ids + (self.id,)

                return ActiveState(
                    active_ids=active_ids,
                    received_ids=n_received_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx for idx, id in assigned_ids.items() if id != self.id
                }

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {state}")


@dataclass
class OnNextAndCompleteTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(
        self,
    ) -> ActiveState | CompletedState | StopContinuationState | ErrorState:
        match state := self.child.get_state():
            case ActiveState(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_received_ids = received_ids + (self.id,)
                n_active_ids = active_ids - {self.id}

                # remaining active Flowables
                if n_active_ids:
                    return ActiveState(
                        active_ids=n_active_ids,
                        received_ids=n_received_ids,
                    )

                else:
                    return CompletedState(
                        assigned_ids={},
                        received_ids=n_received_ids,
                    )

            case CompletedState(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx for idx, id in assigned_ids.items() if id != self.id
                }

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {state}")


@dataclass
class OnCompletedTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(
        self,
    ) -> ActiveState | CompletedState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_active_ids = active_ids - {self.id}

                if n_active_ids:
                    return ActiveState(
                        active_ids=n_active_ids,
                        received_ids=received_ids,
                    )

                else:
                    return CompletedState(
                        assigned_ids={},
                        received_ids=received_ids,
                    )

            case CompletedState(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx for idx, id in assigned_ids.items() if id != self.id
                }

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass(frozen=False)
class OnErrorTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(self) -> CompletedState | StopContinuationState | ErrorState:

        match p_state := self.child.get_state():
            case ActiveState(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx
                    for idx, id in enumerate(
                        id
                        for id in active_ids
                        if id != self.id and id not in received_ids
                    )
                }

                return CompletedState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx for idx, id in assigned_ids.items() if id != self.id
                }

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass
class RequestTransition(MergeStateTransition):
    """Downstream requests new item"""

    child: MergeStateTransition

    @override
    def get_state(self) -> ActiveState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                received_ids=received_ids,
                active_ids=active_ids,
            ):
                n_received_ids = received_ids[1:]

                # todo: assert requested_ids is empty
                return ActiveState(
                    received_ids=n_received_ids,
                    active_ids=active_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                await_id = received_ids[0]
                n_assigned_ids = {
                    id: idx for idx, id in assigned_ids.items() if id != await_id
                }

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[await_id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}.")


@dataclass(frozen=False)
class CancelTransition(MergeStateTransition):
    child: MergeStateTransition

    @override
    def get_state(self) -> CompletedState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {
                    id: idx
                    for idx, id in enumerate(
                        id
                        for id in active_ids
                        if id not in received_ids
                    )
                }

                return CompletedState(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                )

            # Flowable is already completed via on_completed or on_error
            # Certificate provided to the cancel() must be ignored
            case CompletedState():
                return p_state

            case _:
                return ErrorState(f"Unexpected state {p_state}.")
