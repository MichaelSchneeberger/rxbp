from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


class MergeState:
    pass


@dataclass(frozen=True)
class Error(MergeState):
    msg: str


@dataclass(frozen=True)
class Active(MergeState, ABC):
    active_ids: set[int]
    received_ids: tuple[int, ...]


@dataclass(frozen=True)
class Request(Active, ABC):
    id: int


@dataclass(frozen=True)
class Completed(MergeState, ABC):
    assigned_ids: dict[int, int]
    received_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class StopContinuation(Completed):
    index: int






@dataclass(frozen=True, slots=True)
class InitState(MergeState):
    """No subscriptions"""


@dataclass(frozen=True)
class ActiveStateMixin(MergeState):
    # # number of completed upstream observables
    # # State is active as long as n_completed < n_children
    # n_completed: int
    
    # n_children: int

    active_ids: tuple[int, ...]

    # upstream continuation certificates
    certificates: tuple[ContCert, ...]


@dataclass(frozen=True)
class StopContinuationStateMixin:
    """Stop continuations associated with incoming upstream call"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContCert


@dataclass(frozen=True)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """Await upstream items"""


@dataclass(frozen=True, slots=True)
class InitState(AwaitUpstreamStateMixin):
    """Await first upstream item"""


@dataclass(frozen=True, slots=True)
class AwaitOnNextState(StopContinuationStateMixin, AwaitUpstreamStateMixin):
    """Await futher items."""


@dataclass(frozen=True, slots=True)
class AwaitDownstreamStateMixin(ActiveStateMixin):
    """Await downstream request"""

    on_next_calls: tuple[BackpressuredOnNextCalls, ...]


@dataclass(frozen=True, slots=True)
class OnNextState[U](AwaitDownstreamStateMixin):
    """send item"""

    value: U
    observer: DeferredHandler | None


@dataclass(frozen=True, slots=True)
class KeepWaitingState(StopContinuationStateMixin, AwaitDownstreamStateMixin):
    """Await for downstream to request new item"""

    # certificate: ContCert


@dataclass(frozen=True, slots=True)
class TerminatedStateMixin(MergeState):
    """Flowable either completed, errored, or cancelled"""

    # Assign certificate to each active upstream flowable.
    # This is important as the certificate is either used:
    # - to stop calling upsream flowable (on_next, on_next_and_complete, ...), or
    # - to cancel upstream flowable
    certificates: dict[int, ContCert]


@dataclass(frozen=True, slots=True)
class OnNextAndCompleteState[U](TerminatedStateMixin):
    """Send item and complete downstream observer."""

    value: U


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
class CancelledAwaitRequestState(TerminatedStateMixin):
    """Flowable is cancelled, but upstream pending."""

    # The certificate is used to stop the downstream request.
    # It is not used to stop the current upstream.
    certificate: ContCert


@dataclass(frozen=True, slots=True)
class CancelledStopRequestState(StopContinuationStateMixin, TerminatedStateMixin):
    """Flowable is cancelled."""


@dataclass(frozen=True, slots=True)
class HasTerminatedState(StopContinuationStateMixin, TerminatedStateMixin):
    """Has previously been terminated"""
