from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)


class ZipState(ABC): ...


@dataclass
class ActiveState[U](ZipState):
    values: dict[int, U]
    observers: dict[int, DeferredObserver]


@dataclass
class WaitState(ActiveState):
    certificates: tuple[ContinuationCertificate, ...]


@dataclass
class WaitFurtherItemsState(WaitState):
    certificate: ContinuationCertificate


@dataclass
class OnNextState(ActiveState):
    pass


@dataclassabc
class NotActiveState(ZipState):
    certificate: ContinuationCertificate
    certificates: tuple[ContinuationCertificate, ...]


@dataclassabc
class OnErrorState(NotActiveState):
    exception: Exception


@dataclassabc
class OnCompleteState(NotActiveState):
    pass


@dataclassabc
class HasTerminatedState(NotActiveState):
    pass
