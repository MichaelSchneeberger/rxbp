from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate


class ShareState:
    pass


@dataclass(frozen=False)
class ActiveStateMixin(ShareState):
    # buffer indices are modified on ack
    buffer_map: dict[int, int]

    first_buffer_index: int
    last_buffer_index: int

    # # last_buffer_index+1 when element
    # is_ack: bool

    weights: dict[int, int]

    requested_certificates: dict[int, ContinuationCertificate]
    cancelled_certificates: dict[int, ContinuationCertificate]

    is_completed: bool


@dataclass(frozen=False)
class StopContinuationStateMixin:
    """Stop continuations associated with incoming upstream call"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContinuationCertificate


@dataclass(frozen=False)
class AdjustBufferStateMixin(ActiveStateMixin):
    n_buffer_pop: int


@dataclass(frozen=False)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """At least one downstream observer requested a new item."""


@dataclass(frozen=False)
class AwaitUpstreamAdjustBufferState(AdjustBufferStateMixin, AwaitUpstreamStateMixin):
    # n_buffer_pop: int
    pass


@dataclass(frozen=False)
class InitState(AwaitUpstreamStateMixin):
    pass


@dataclass(frozen=False, slots=True)
class RequestUpstream(AwaitUpstreamStateMixin):
    # upstream_ack_observer: DeferredHandler
    # certificate: ContinuationCertificate
    pass


@dataclass(frozen=False, slots=True)
class AwaitOnNext(AwaitUpstreamStateMixin):
    certificate: ContinuationCertificate


@dataclass(frozen=False, slots=True)
class OnNextFromBuffer(AwaitUpstreamStateMixin):
    index: int
    pop_item: bool


@dataclass(frozen=False)
class AwaitDownstreamStateMixin(ActiveStateMixin):
    # cancelled_certificates: dict[int, ContinuationCertificate]
    pass


@dataclass(frozen=False)
class AwaitDownstreamAdjustBufferState(AdjustBufferStateMixin, AwaitDownstreamStateMixin):
    # n_buffer_pop: int
    pass


@dataclass(frozen=False)
class CancelledKeepAwaitDownstreamState(StopContinuationStateMixin, AwaitDownstreamStateMixin):
    pass


@dataclass(frozen=False, slots=True)
class OnNext(AwaitDownstreamStateMixin):
    send_ids: tuple[int, ...]
    buffer_item: bool


@dataclass(frozen=True, slots=True)
class TerminatedStateMixin(ShareState):
    requested_certificates: dict[int, ContinuationCertificate]


# @dataclass(frozen=False, slots=True)
# class OnNextAndComplete(AwaitDownstreamStateMixin):
#     send_ids: tuple[int, ...]
#     buffer_item: bool


@dataclass(frozen=True, slots=True)
class OnCompletedState(TerminatedStateMixin):
    send_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ErrorStateMixin(TerminatedStateMixin):
    exception: Exception


@dataclass(frozen=True, slots=True)
class OnErrorState(ErrorStateMixin):
    send_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class HasErroredState(ErrorStateMixin):
    pass


@dataclass(frozen=True, slots=True)
class CancelledState(TerminatedStateMixin):
    pass


@dataclass(frozen=True, slots=True)
class HasTerminatedState(TerminatedStateMixin):
    pass
