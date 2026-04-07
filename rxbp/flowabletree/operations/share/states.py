from __future__ import annotations

from dataclasses import dataclass

from contcert.typing import ContCert

"""
States
------

State groups (-), states (>):
- Active - AwaitUpstream
  > Init
  > RequestUpstream
  > AwaitOnNext
  > OnNextFromBuffer
  > AwaitUpstreamAfterCancel
- Active - AwaitDownstream
  > OnNext
  > AwaitDownstreamAfterCancel
- Terminated
  > OnCompleted
  > Cancelled
  > HasTerminated
- Terminated - Errored
  > OnError
  > HasErrored
"""


class ShareState:
    pass


@dataclass(frozen=False)
class ActiveStateMixin(ShareState):
    """Further items may be pending to be sent."""

    # Maps each downstream ID to its current (virtual) buffer index.
    # This map determines to which downstream observers new items are sent to.
    buffer_map: dict[int, int]

    # The first buffer index is used to map the virtual buffer index to the actual buffer index.
    # Explanation: Old and unused items are removed from the buffer without updating the indices
    # in the buffer_map. Instead, only the first buffer index is updated, pointing to the first
    # virtual index of the buffer.
    first_buffer_index: int

    # The last buffer index determines if a new item is taken from the buffer, or requested from upstream.
    last_buffer_index: int

    # Store downstream virtual_counts, as the downstream virtual_counts are dynamically adjusted.
    virtual_counts: dict[int, int]

    # Containers that hold certificates for each downstream observer that is either cancelled,
    # or does not yet point to the last buffer item.
    # These certificates are either consumed when downstream observers request a new item, or
    # when receiving an upstream item.
    cancelled_certificates: dict[int, ContCert]
    requested_certificates: dict[int, ContCert]

    is_completed: bool


@dataclass(frozen=False)
class StopContinuationStateMixin:
    """Stop continuing exeuction"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContCert


@dataclass(frozen=False)
class RemoveBufferItemsStateMixin(ActiveStateMixin):
    """Delete first n items from the buffer"""

    n_buffer_pop: int


@dataclass(frozen=False)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """At least one downstream observer requested a new item"""


@dataclass(frozen=False)
class InitState(AwaitUpstreamStateMixin):
    pass


@dataclass(frozen=False, slots=True)
class RequestUpstream(AwaitUpstreamStateMixin):
    """Request new upstream item on first downstream request"""

    pass


@dataclass(frozen=False, slots=True)
class AwaitOnNext(AwaitUpstreamStateMixin):
    """New upstream item has already been requested, await on_next call"""

    certificate: ContCert


@dataclass(frozen=False, slots=True)
class OnNextFromBuffer(AwaitUpstreamStateMixin):
    """Send item from buffer."""

    index: int
    pop_item: bool


@dataclass(frozen=False)
class AwaitUpstreamAfterCancelState(
    RemoveBufferItemsStateMixin, AwaitUpstreamStateMixin
):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""

    pass


@dataclass(frozen=False)
class AwaitDownstreamStateMixin(ActiveStateMixin):
    """Newest upstream item has benn sent downstream, await downstream request."""

    pass


@dataclass(frozen=False)
class AwaitDownstreamAfterCancelState(
    RemoveBufferItemsStateMixin, AwaitDownstreamStateMixin
):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""

    pass


@dataclass(frozen=False)
class RequestFromCancelledDownstreamState(
    StopContinuationStateMixin, AwaitDownstreamStateMixin
):
    """New item requested after downstream was cancelled, stop continuation using the certificate."""

    pass


@dataclass(frozen=False, slots=True)
class OnNext(AwaitDownstreamStateMixin):
    """New item received."""

    # IDs referencing downstream observers that poin to the last item in the buffer
    send_ids: tuple[int, ...]

    buffer_item: bool


@dataclass(frozen=True, slots=True)
class TerminatedStateMixin(ShareState):
    requested_certificates: dict[int, ContCert]


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
