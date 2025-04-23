from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate


@dataclass
class ShareState:
    pass


@dataclass
class ActiveState(ShareState):
    # buffer indices are modified on ack
    buffer_map: dict[int, int]

    first_buffer_index: int
    last_buffer_index: int

    # last_buffer_index+1 when element
    is_ack: bool

    acc_certificate: ContinuationCertificate

    is_completed: bool


@dataclass
class AwaitOnNextBase(ActiveState):
    """At least one downstream observer requested a new item."""

    pass


class InitState(AwaitOnNextBase):
    pass


@dataclass(frozen=False)
class AckUpstream(AwaitOnNextBase):
    # upstream_ack_observer: DeferredObserver
    certificate: ContinuationCertificate


@dataclass
class AwaitOnNext(AwaitOnNextBase):
    certificate: ContinuationCertificate


@dataclass
class SendItemFromBuffer(AwaitOnNextBase):
    index: int
    pop_item: bool


@dataclass
class SendItem(ActiveState):
    send_ids: tuple[int, ...]
    buffer_item: bool


@dataclass
class CompleteState(ShareState):
    send_ids: tuple[int, ...]
    acc_certificate: ContinuationCertificate


@dataclass
class TerminatedBaseState(ShareState):
    exception: Exception


@dataclass
class TerminatedState(TerminatedBaseState):
    pass


@dataclass
class ErrorState(TerminatedBaseState):
    # exception: Exception
    send_ids: tuple[int, ...]
    acc_certificate: ContinuationCertificate


@dataclass
class CancelledState(ShareState):
    certificate: ContinuationCertificate
