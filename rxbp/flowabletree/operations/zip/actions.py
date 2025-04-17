from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

from rxbp.flowabletree.operations.zip.states import (
    NotActiveState,
    HasTerminatedState,
    OnCompleteState,
    OnErrorState,
    OnNextState,
    WaitFurtherItemsState,
    ZipState,
    WaitState,
)


class ZipAction(ABC):
    @abstractmethod
    def get_state(self) -> ZipState: ...


@dataclass
class WaitAction(ZipAction):
    values: dict[int, None]
    observers: dict[int, DeferredObserver]
    certificates: tuple[ContinuationCertificate, ...]

    def get_state(self):
        return WaitState(
            values=self.values,
            observers=self.observers,
            certificates=self.certificates,
        )


class SingleChildNodeAction(ZipAction):
    @property
    @abstractmethod
    def child(self) -> ZipAction: ...


@dataclassabc
class OnNextAction[U](SingleChildNodeAction):
    child: ZipAction
    id: int
    n_children: int
    value: U
    observer: DeferredObserver

    def get_state(self):
        match state := self.child.get_state():
            case WaitFurtherItemsState() | WaitState():
                values = state.values | {self.id: self.value}
                observers = state.observers | {self.id: self.observer}

                if len(values) == self.n_children:
                    return OnNextState(
                        values=values,
                        observers=observers,
                    )

                else:
                    return WaitFurtherItemsState(
                        certificate=state.certificates[0],
                        certificates=state.certificates[1:],
                        values=values,
                        observers=observers,
                    )
            
            case NotActiveState(certificates=certificates):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
                
            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnNextAndCompleteAction[U](SingleChildNodeAction):
    child: ZipAction
    id: int
    n_children: int
    value: U

    def get_state(self):
        match state := self.child.get_state():
            case WaitFurtherItemsState() | WaitState():
                values = state.values | {self.id: self.value}
                observers = state.observers

                if len(values) == self.n_children:
                    return OnNextState(
                        values=values,
                        observers=observers,
                    )

                else:
                    return WaitFurtherItemsState(
                        certificate=state.certificates[0],
                        certificates=state.certificates[1:],
                        values=values,
                        observers=observers,
                    )

            case NotActiveState(certificates=certificates):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnCompletedAction(SingleChildNodeAction):
    child: ZipAction

    def get_state(self):
        match state := self.child.get_state():
            case WaitFurtherItemsState(certificates=certificates):
                return OnCompleteState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
            
            case NotActiveState(certificates=certificates):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
            
            case _:
                raise Exception(f"Unknown state {state}")


@dataclassabc
class OnErrorAction(SingleChildNodeAction):
    child: ZipAction
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case WaitFurtherItemsState(certificates=certificates):
                return OnErrorState(
                    exception=self.exception,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
            
            case NotActiveState(certificates=certificates):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
            
            case _:
                raise Exception(f"Unknown state {state}")
