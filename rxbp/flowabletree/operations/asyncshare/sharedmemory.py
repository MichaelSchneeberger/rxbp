from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from continuationmonad.typing import DeferredHandler, ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class ShareSharedMemory[V]:
    # # The certificate returned when subscribing to the upstream observer.
    # subscribe_certificate: ContinuationCertificate

    # Todo: include in the InitState
    # The cancellation returned when subscribing to the upstream observer.
    upstream_cancellation: Cancellable | None

    # modified synchronously
    deferred_handler: DeferredHandler | None

    # The transition is updated by acquiring the lock 'lock'.
    lock: Lock
    transition: ShareStateTransition

    # The buffer is updated by acquiring the lock 'buffer_lock'
    buffer_lock: Lock
    first_index: int
    buffer: list[V]

    def get_buffer_item(self, index: int):
        with self.buffer_lock:
            rel_index = index - self.first_index
            assert 0 <= rel_index <= len(self.buffer), (
                f"No item in buffer at {index-self.first_index=}."
            )
            return self.buffer[index - self.first_index]

    def pop_buffer(self, num: int):
        with self.buffer_lock:
            del self.buffer[:num]
            self.first_index += num
