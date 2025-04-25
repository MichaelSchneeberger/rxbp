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
    # Depending on the provided selector, some upstream flowables are hold back (backpressured).
    # In this case, observers and values attribute are initially not empty.

    # - deferred continuationmonad observers
    observers: dict[int, DeferredObserver]

    # - use previous values instead
    values: dict[int, U]


@dataclass
class AwaitOnNextState(ActiveState):
    # equals the number of active upstream flowables minus one
    certificates: tuple[ContinuationCertificate, ...]

    is_completed: bool


@dataclass
class RequestState(AwaitOnNextState):
    """Request a new item from n upstream flowables"""

    pass


@dataclass
class AwaitFurtherState(AwaitOnNextState):
    # used to stop calling upstream flowable
    certificate: ContinuationCertificate


@dataclass
class OnNextState(ActiveState):
    pass


@dataclassabc
class TerminatedBaseState(ZipState):
    """Flowable either completed or errored"""

    # certificates required to stop upstream flowables
    # certificates: tuple[ContinuationCertificate, ...]
    certificates: dict[int, ContinuationCertificate]

    # needed to cancel upstream flowables
    # awaiting_ids: tuple[int, ...]


@dataclass
class OnNextAndCompleteState(ActiveState, TerminatedBaseState):
    pass


@dataclassabc
class OnCompletedState(TerminatedBaseState):
    pass


@dataclassabc
class OnErrorState(TerminatedBaseState):
    exception: Exception


@dataclassabc
class HasTerminatedState(TerminatedBaseState):
    """Has previously either completed or errored"""
    certificate: ContinuationCertificate


@dataclass
class CancelledBaseState(TerminatedBaseState):
    pass
    # Assign certificate to each upstream flowable.
    # This is important as the certificate is either consumed:
    # - when upstream calls Zip observer methods, or
    # - when cancelling happens upstream
    # certificates: dict[int, ContinuationCertificate]


@dataclass
class CancelledAwaitRequestState(CancelledBaseState):
    pass
    # no active upstream, await next request
    # certificate: ContinuationCertificate


@dataclass
class CancelState(CancelledBaseState):
    pass


# @dataclass
# class HasCancelledState(CancelledBaseState):
#     certificate: ContinuationCertificate
