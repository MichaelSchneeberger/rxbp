from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate
from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.concatmap.states import CancelledState, ConcatMapState, ActiveState


class ConcatMapStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ConcatMapState: ...


@dataclass(frozen=True)
class ToStateTransition(ConcatMapStateTransition):
    """ Transitions to predefined state """
    
    state: ConcatMapState

    def get_state(self):
        return self.state


@dataclass(frozen=False)
class UpdateCancellableTransition(ConcatMapStateTransition):
    child: ConcatMapStateTransition
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
class CancelTransition(ConcatMapStateTransition):
    child: ConcatMapStateTransition
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
