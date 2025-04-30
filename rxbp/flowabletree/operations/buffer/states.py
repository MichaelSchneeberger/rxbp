from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate


@dataclass
class BufferState:
    pass


@dataclass
class LoopActive(BufferState):
    """Either the loop is already running or it needs to be started"""

    num_items: int
    is_completed: bool

    # since the loop is active, we get an additional certificate
    certificate: ContinuationCertificate


@dataclass
class LoopActivePopBuffer(LoopActive):
    pass


@dataclass
class SendItemAndStartLoop(LoopActive):
    pass


@dataclass
class LoopInactive(BufferState):
    """Either the loop is not running or it needs to be stopped"""
    pass


@dataclass
class SendItemAndComplete(LoopInactive):
    pass


@dataclass
class StopLoop(LoopInactive):
    certificate: ContinuationCertificate


@dataclass
class CompleteState(BufferState):
    pass


@dataclass
class ErrorState(BufferState):
    certificate: ContinuationCertificate
    exception: Exception


@dataclass
class SendErrorState(BufferState):
    exception: Exception


@dataclass
class CancelledState(BufferState):
    # certificate: ContinuationCertificate
    pass


@dataclass
class CancelLoopState(BufferState):
    certificate: ContinuationCertificate
