from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

type UpstreamID = int


@dataclass(frozen=True)
class OnNextPreState[U]:
    id: UpstreamID
    value: U
    subscription: DeferredObserver | None
    n_completed: int


@dataclass(frozen=True)
class MergeState:
    pass


@dataclass(frozen=True)
class ActiveState(MergeState):
    # number of completed upstream observables
    n_completed: int

    # upstream continuation certificates
    certificates: tuple[ContinuationCertificate, ...]


@dataclass(frozen=True)
class AwaitNextBaseState(ActiveState):
    """Wait for upstream item"""


@dataclass(frozen=True)
class AwaitAckBaseState(ActiveState):
    """Wait for downstream ack"""

    acc_states: tuple[OnNextPreState]


@dataclass(frozen=True)
class AwaitNextState(AwaitNextBaseState):
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class OnNextState[U](AwaitAckBaseState):
    """send item"""
    value: U
    subscription: DeferredObserver


@dataclass(frozen=True)
class OnNextNoAckState[U](AwaitNextBaseState):
    """send item"""
    acc_states: tuple[OnNextPreState]
    value: U
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class AwaitAckState(AwaitAckBaseState):
    """there are other items in the buffer to be sent"""
    certificate: ContinuationCertificate


class CompletedBaseState(MergeState):
    pass


@dataclass(frozen=True)
class OnNextAndCompleteState[U](CompletedBaseState):
    """send item and complete downstream"""

    value: U


class CompleteState(CompletedBaseState):
    pass


@dataclass(frozen=True)
class ErrorBaseState(MergeState):
    certificates: tuple[ContinuationCertificate, ...]
    awaiting_ids: tuple[UpstreamID, ...]


@dataclass(frozen=True)
class ErrorState(ErrorBaseState):
    exception: Exception


@dataclass(frozen=True)
class TerminatedState(ErrorBaseState):
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class CancelledState(MergeState):
    certificates: dict[UpstreamID, ContinuationCertificate]
