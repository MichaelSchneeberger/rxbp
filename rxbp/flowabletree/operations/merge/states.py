from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

type UpstreamID = int


@dataclass(frozen=True)
class BackpressuredOnNextCalls[U]:
    id: UpstreamID
    value: U
    observer: DeferredObserver | None
    # completed: bool


@dataclass(frozen=True)
class MergeState:
    pass


@dataclass(frozen=True)
class ActiveStateMixin(MergeState):
    # number of completed upstream observables
    # State is active as long as n_completed < n_children
    n_completed: int

    # upstream continuation certificates
    certificates: tuple[ContinuationCertificate, ...]


@dataclass(frozen=True)
class StopContinuationStateMixin:
    """Stop continuations associated with incoming upstream call"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """Await upstream items"""


@dataclass(frozen=True)
class InitState(AwaitUpstreamStateMixin):
    """Await first upstream item"""


@dataclass(frozen=True)
class AwaitOnNextState(StopContinuationStateMixin, AwaitUpstreamStateMixin):
    """Await futher items."""


@dataclass(frozen=True)
class AwaitDownstreamStateMixin(ActiveStateMixin):
    """Await downstream request"""

    on_next_calls: tuple[BackpressuredOnNextCalls, ...]


@dataclass(frozen=True)
class OnNextState[U](AwaitDownstreamStateMixin):
    """send item"""

    value: U
    observer: DeferredObserver | None


@dataclass(frozen=True)
class KeepWaitingState(StopContinuationStateMixin, AwaitDownstreamStateMixin):
    """Await for downstream to request new item"""

    # certificate: ContinuationCertificate


@dataclass(frozen=True)
class TerminatedStateMixin(MergeState):
    """Flowable either completed, errored, or cancelled"""

    # Assign certificate to each active upstream flowable.
    # This is important as the certificate is either used:
    # - to stop calling upsream flowable (on_next, on_next_and_complete, ...), or
    # - to cancel upstream flowable
    certificates: dict[int, ContinuationCertificate]


@dataclass(frozen=True)
class OnNextAndCompleteState[U](TerminatedStateMixin):
    """Send item and complete downstream observer."""

    value: U


@dataclass(frozen=True)
class OnCompletedState(TerminatedStateMixin):
    """Complete downstream observer."""


@dataclass(frozen=True)
class OnErrorState(TerminatedStateMixin):
    """Error downstream observer."""

    exception: Exception


@dataclass(frozen=True)
class CancelledState(TerminatedStateMixin):
    """Flowable is cancelled."""


@dataclass(frozen=True)
class CancelledAwaitRequestState(TerminatedStateMixin):
    """Flowable is cancelled, but upstream pending."""

    # The certificate is used to stop the downstream request.
    # It is not used to stop the current upstream.
    certificate: ContinuationCertificate


@dataclass(frozen=True)
class CancelledStopRequestState(StopContinuationStateMixin, TerminatedStateMixin):
    """Flowable is cancelled."""


@dataclass(frozen=True)
class HasTerminatedState(StopContinuationStateMixin, TerminatedStateMixin):
    """Has previously been terminated"""
