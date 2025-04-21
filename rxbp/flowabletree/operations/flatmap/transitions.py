from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate
from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.flatmap.states import CancelledState, FlatMapState, ActiveState


# Actions
#########


class FlatMapTransition(ABC):
    @abstractmethod
    def get_state(self) -> FlatMapState: ...


@dataclass(frozen=True)
class ToStateTransition(FlatMapTransition):
    """ Transitions to predefined state """
    
    state: FlatMapState

    def get_state(self):
        return self.state


@dataclass(frozen=False)
class UpdateCancellableAction(FlatMapTransition):
    child: FlatMapTransition
    cancellable: Cancellable

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState():
                return ActiveState(
                    cancellable=self.cancellable,
                )

            case CancelledState(certificate=certificate):
                return CancelledState(
                    certificate=certificate,
                    cancellable=self.cancellable,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class CancelAction(FlatMapTransition):
    child: FlatMapTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState(cancellable=cancellable):
                return CancelledState(
                    certificate=self.certificate,
                    cancellable=cancellable,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")
