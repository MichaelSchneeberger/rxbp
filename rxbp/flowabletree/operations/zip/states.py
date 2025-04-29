from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredHandler,
)


class ZipState(ABC): ...


@dataclass(frozen=True)
class ActiveStateMixin[U](ZipState):
    """
    Represents states where the Zip operator is active.

    Depending on the provided selector, some upstream flowables are hold back (backpressured).
    In this case, observers and values attribute are initially not empty.
    """

    # Deferred continuationmonad observers used to request a new item from inactive upstream flowables.
    observers: dict[int, DeferredHandler]

    # Received values
    values: dict[int, U]


@dataclass(frozen=True)
class StopContinuationStateMixin:
    """Stop continuations associated with incoming upstream call"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """Represents states where the Zip operator is awaiting upstream items."""

    # Certificates returned when requesting new upstream item, one is returned to downstream flowable during subscription.
    # Hence, the number qquals the number of active upstream flowables minus one.
    certificates: tuple[ContinuationCertificate, ...]

    # Zip operator is scheduled to complete when all upstream items are received.
    is_completed: bool


@dataclass(frozen=True, slots=True)
class AwaitOnNextState(AwaitUpstreamStateMixin):
    """Request a new item from all non-backpressured upstream flowables"""


@dataclass(frozen=True, slots=True)
class AwaitFurtherState(StopContinuationStateMixin, AwaitUpstreamStateMixin):
    """At least one upstream item received, await futher items."""


@dataclass(frozen=True, slots=True)
class OnNextState(ActiveStateMixin):
    """Send item downstream."""


@dataclass(frozen=True, slots=True)
class TerminatedStateMixin(ZipState):
    """Flowable either completed, errored, or cancelled"""

    # Assign certificate to each active upstream flowable.
    # This is important as the certificate is either used:
    # - to stop calling upsream flowable (on_next, on_next_and_complete, ...), or
    # - to cancel upstream flowable
    certificates: dict[int, ContinuationCertificate]


@dataclass(frozen=True, slots=True)
class OnNextAndCompleteState(ActiveStateMixin, TerminatedStateMixin):
    """Send item and complete downstream observer."""


@dataclass(frozen=True, slots=True)
class OnCompletedState(TerminatedStateMixin):
    """Complete downstream observer."""


@dataclass(frozen=True, slots=True)
class OnErrorState(TerminatedStateMixin):
    """Error downstream observer."""

    exception: Exception


@dataclass(frozen=True, slots=True)
class CancelledState(TerminatedStateMixin):
    """Flowable is cancelled."""


@dataclass(frozen=True, slots=True)
class CancelledAwaitRequestState(ZipState):
    """Flowable is cancelled, but downstream request pending."""

    certificate: ContinuationCertificate


@dataclass(frozen=True, slots=True)
class HasTerminatedState(StopContinuationStateMixin, TerminatedStateMixin):
    """Has previously been terminated"""
