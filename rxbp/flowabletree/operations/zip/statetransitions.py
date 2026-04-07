from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import override

from rxbp.flowabletree.operations.zip.states import (
    StopContinuation,
    Completed,
    Active,
    ZipState,
    Error,
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
    def get_state(self) -> Active | StopContinuation | Error:
        match p_state := self.child.get_state():
            case Active(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}

                return Active(
                    requested_ids=n_requested_ids,
                    completed_ids=completed_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    index=self.id,
                )

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class OnNextAndCompleteTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case Active(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}
                n_completed_ids = completed_ids | {self.id}

                return Active(
                    requested_ids=n_requested_ids,
                    completed_ids=n_completed_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    index=self.id,
                )

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class OnCompletedTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case Active(
                requested_ids=requested_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(requested_ids) if id != self.id}

                return Completed(
                    assigned_ids=n_assigned_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    index=self.id,
                )

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class RequestTransition(ZipStateTransition):
    """Downstream requests new item"""

    child: ZipStateTransition
    requested_ids: set[int]

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case Active(completed_ids=completed_ids):
                # todo: assert requested_ids is empty
                return Active(
                    requested_ids=self.requested_ids,
                    completed_ids=completed_ids,
                )

            case Completed():
                # todo: assert assigned_ids is empty
                return Completed(assigned_ids={})

            case _:
                return Error(f"Unexpected state {p_state}.")


@dataclass
class CancelTransition(ZipStateTransition):
    child: ZipStateTransition

    @override
    def get_state(self) -> Completed | Error:
        match p_state := self.child.get_state():
            case Active(
                requested_ids=requested_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(requested_ids)}

                return Completed(
                    assigned_ids=n_assigned_ids,
                )

            # Flowable is already completed via on_completed or on_error
            # Certificate provided to the cancel() can be ignored
            case Completed():
                return p_state

            case _:
                return Error(f"Unexpected state {p_state}.")

