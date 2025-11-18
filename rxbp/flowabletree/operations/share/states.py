from __future__ import annotations

import functools

from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

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

    # Maps each downstream ID to its current absolute buffer index.
    # This map determines (during on_next call) to which downstream observers new items are sent to.
    # It is updated when a downstream observer requests the next item, a buffer map is removed when downstream cancels.
    buffer_map: dict[int, int]

    # Unused items are removed from the buffer without updating the absolute buffer indices.
    # Instead, only the first_buffer_index is updated, pointing to the first item in the buffer.
    # It is updated when the last downstream observer that points to the first buffer item requests a new item or cancels.
    first_buffer_index: int

    # The last_buffer_index determines if either the next downstream item is taken from the buffer or requested from upstream.
    # It is updated when receiving on_next call.
    last_buffer_index: int

    # # Stores downstream weights, determined the weight requested from upstream
    # # The weights are updated when downstream requests the next item, weights are removed when downstream cancels.
    # weights: dict[int, int]

    # Stores certificates from requesting next item from upstream observer.
    requested_certificates: dict[int, ContinuationCertificate]

    # cancelled_certificates: dict[int, ContinuationCertificate]

    is_completed: bool

    def is_cancelled(self, id: int):
        return id not in self.buffer_map

    def get_synced_ids(self):
        return tuple(
            id
            for id, index in self.buffer_map.items()
            if index == self.last_buffer_index
        )


@dataclass(frozen=False)
class StopContinuationStateMixin:
    """Stop continuing exeuction"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContinuationCertificate


@dataclass(frozen=False)
class RemoveBufferItemsStateMixin(ActiveStateMixin):
    """Delete first n items from the buffer"""

    # n_buffer_pop: int


@dataclass(frozen=False)
class AwaitUpstreamStateMixin(ActiveStateMixin):
    """At least one downstream observer requested a new item"""

    # # Stores certificates from requesting next item from upstream observer.
    # requested_certificates: dict[int, ContinuationCertificate]

    def is_awaiting(self, id: int):
        return self.last_buffer_index + 1 == self.buffer_map[id]


@dataclass(frozen=False)
class AwaitDownstreamStateMixin(ActiveStateMixin):
    """
    on_next upstream call received, await first synced downstream request.
    There are no valid certificates stored in the shared observer.
    """

    # """Newest upstream item has been sent downstream, await downstream request."""

    def is_synced(self, id: int):
        return self.last_buffer_index == self.buffer_map[id]

    @property
    def buffer_item(self):
        return self.first_buffer_index != self.last_buffer_index


@dataclass(frozen=False)
class InitState(AwaitUpstreamStateMixin):
    # certificates: tuple[ContinuationCertificate]
    # upstream_cancellation: Cancellable
    pass


@dataclass(frozen=False, slots=True)
class RequestUpstream(AwaitUpstreamStateMixin):
    """Request new upstream item on first downstream request"""

    pass


@dataclass(frozen=False, slots=True)
class AwaitOnNext(AwaitUpstreamStateMixin):
    """New upstream item has already been requested, await on_next call"""

    certificate: ContinuationCertificate


@dataclass(frozen=False, slots=True)
class OnNextFromBuffer(AwaitUpstreamStateMixin):
    """Send item from buffer."""

    index: int

    # remove first item from buffer if the current downstream observer exclusively points
    # (i.e., no one else)  the first item in the buffer.
    pop_item: bool


@dataclass(frozen=False)
class AwaitUpstreamAfterCancelState(
    RemoveBufferItemsStateMixin, AwaitUpstreamStateMixin
):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""


@dataclass(frozen=False)
class AwaitDownstreamAfterCancelState(
    RemoveBufferItemsStateMixin, AwaitDownstreamStateMixin
):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""


@dataclass(frozen=False)
class RequestFromCancelledDownstreamState(
    StopContinuationStateMixin, AwaitDownstreamStateMixin
):
    """New item requested after downstream was cancelled, stop continuation using the certificate."""


@dataclass(frozen=False, slots=True)
class OnNext(AwaitDownstreamStateMixin):
    """New item received."""

    # IDs referencing downstream observers that poin to the last item in the buffer
    # send_ids: tuple[int, ...]

    # buffer_item: bool


@dataclass(frozen=True, slots=True)
class TerminatedStateMixin(ShareState):
    pass


@dataclass(frozen=True, slots=True)
class OnCompletedState(TerminatedStateMixin):
    send_ids: tuple[int, ...]
    requested_certificates: dict[int, ContinuationCertificate]
    # pass


# @dataclass(frozen=True, slots=True)
# class ErrorStateMixin(TerminatedStateMixin):
#     exception: Exception


@dataclass(frozen=True, slots=True)
class OnErrorState(TerminatedStateMixin):
    send_ids: tuple[int, ...]
    requested_certificates: dict[int, ContinuationCertificate]
    # pass


# @dataclass(frozen=True, slots=True)
# class HasErroredState(ErrorStateMixin):
#     pass


@dataclass(frozen=True, slots=True)
class CancelledState(TerminatedStateMixin):
    # requested_certificates: dict[int, ContinuationCertificate]
    certificate: ContinuationCertificate


# @dataclass(frozen=True, slots=True)
# class HasTerminatedState(TerminatedStateMixin):
#     pass
