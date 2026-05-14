from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import override

from rxbp.flowabletree.operations.zip.states import (
    StopContinuationState,
    CompletedState,
    ActiveState,
    ZipState,
    ErrorState,
)


class ZipStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ZipState: ...


@dataclass(frozen=True)
class ToStateTransition(ZipStateTransition):
    """Transitions to predefined state"""

    state: ZipState

    @override
    def get_state(self):
        return self.state


@dataclass
class OnNextTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self) -> ActiveState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}

                return ActiveState(
                    requested_ids=n_requested_ids,
                    completed_ids=completed_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass
class OnNextAndCompleteTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self) -> ActiveState | CompletedState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}
                n_completed_ids = completed_ids | {self.id}

                return ActiveState(
                    requested_ids=n_requested_ids,
                    completed_ids=n_completed_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass
class OnCompletedTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self) -> ActiveState | CompletedState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                requested_ids=requested_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(id for id in requested_ids if id != self.id)}

                return CompletedState(
                    assigned_ids=n_assigned_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass
class OnErrorTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self) -> CompletedState | StopContinuationState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                requested_ids=requested_ids,
            ):
                # For zip, if one fails, we probably want to cancel all others
                n_assigned_ids = {id: idx for idx, id in enumerate(id for id in requested_ids if id != self.id)}

                return CompletedState(
                    assigned_ids=n_assigned_ids,
                )

            case CompletedState(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuationState(
                    assigned_ids=n_assigned_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return ErrorState(f"Unexpected state {p_state}")


@dataclass
class RequestTransition(ZipStateTransition):
    """Downstream requests new item"""

    child: ZipStateTransition
    requested_ids: set[int]

    @override
    def get_state(self) -> ActiveState | CompletedState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(completed_ids=completed_ids):
                # todo: assert requested_ids is empty
                return ActiveState(
                    requested_ids=self.requested_ids,
                    completed_ids=completed_ids,
                )

            case CompletedState():
                # todo: assert assigned_ids is empty
                return CompletedState(assigned_ids={})

            case _:
                return ErrorState(f"Unexpected state {p_state}.")


@dataclass
class CancelTransition(ZipStateTransition):
    child: ZipStateTransition

    @override
    def get_state(self) -> CompletedState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                requested_ids=requested_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(requested_ids)}

                return CompletedState(
                    assigned_ids=n_assigned_ids,
                )

            # Flowable is already completed via on_completed or on_error
            # Certificate provided to the cancel() must be ignored
            case CompletedState():
                return p_state

            case _:
                return ErrorState(f"Unexpected state {p_state}.")
