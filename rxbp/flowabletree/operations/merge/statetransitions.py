from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import override


from rxbp.flowabletree.operations.merge.states import (
    Active,
    Completed,
    Error,
    StopContinuation,
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
    def get_state(self) -> Active | StopContinuation | Error:
        match state := self.child.get_state():
            case Active(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_received_ids = received_ids + (self.id,)

                return Active(
                    active_ids=active_ids,
                    received_ids=n_received_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return Error(f"Unexpected state {state}")


@dataclass
class OnNextAndCompleteTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(self) -> Active | Completed | StopContinuation | Error:
        match state := self.child.get_state():
            case Active(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_received_ids = received_ids + (self.id,)
                n_active_ids = active_ids - {self.id}

                # remaining active Flowables
                if n_active_ids:
                    return Active(
                        active_ids=n_active_ids,
                        received_ids=n_received_ids,
                    )

                else:
                    return Completed(
                        assigned_ids={},
                        received_ids=n_received_ids,
                    )

            case Completed(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return Error(f"Unexpected state {state}")


@dataclass
class OnCompletedTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(self) -> Active | Completed | StopContinuation | Error:
        match p_state := self.child.get_state():
            case Active(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_active_ids = active_ids - {self.id}

                if n_active_ids:
                    return Active(
                        active_ids=n_active_ids,
                        received_ids=received_ids,
                    )

                else:
                    return Completed(
                        assigned_ids={},
                        received_ids=received_ids,
                    )

            case Completed(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass(frozen=False)
class OnErrorTransition(MergeStateTransition):
    child: MergeStateTransition
    id: int

    @override
    def get_state(self) -> Completed | StopContinuation | Error:

        match p_state := self.child.get_state():
            case Active(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(active_ids) if id != self.id and id not in received_ids}

                return Completed(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[self.id],
                )

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class RequestTransition(MergeStateTransition):
    """Downstream requests new item"""

    child: MergeStateTransition

    @override
    def get_state(self) -> Active | StopContinuation | Error:
        match p_state := self.child.get_state():
            case Active(
                received_ids=received_ids,
                active_ids=active_ids,
            ):
                n_received_ids = received_ids[1:]

                # todo: assert requested_ids is empty
                return Active(
                    received_ids=n_received_ids,
                    active_ids=active_ids,
                )

            case Completed(
                assigned_ids=assigned_ids,
                received_ids=received_ids,
            ):
                await_id = received_ids[0]
                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != await_id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                    index=assigned_ids[await_id],
                )

            case _:
                return Error(f"Unexpected state {p_state}.")


@dataclass(frozen=False)
class CancelTransition(MergeStateTransition):
    child: MergeStateTransition

    @override
    def get_state(self) -> Completed | Error:
        match p_state := self.child.get_state():
            case Active(
                active_ids=active_ids,
                received_ids=received_ids,
            ):
                n_assigned_ids = {id: idx for idx, id in enumerate(active_ids) if id not in received_ids}

                return Completed(
                    assigned_ids=n_assigned_ids,
                    received_ids=received_ids,
                )

            # Flowable is already completed via on_completed or on_error
            # Certificate provided to the cancel() must be ignored
            case Completed():
                return p_state

            case _:
                return Error(f"Unexpected state {p_state}.")

