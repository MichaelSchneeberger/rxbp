from __future__ import annotations

from dataclasses import dataclass
from threading import Lock, RLock

from continuationmonad.typing import (
    DeferredObserver,
)

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.share.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class ShareSharedMemory[V]:
    total_weight: int

    upstream_cancellation: Cancellable

    # modified synchronously
    deferred_observer: DeferredObserver

    # modified using lock
    lock: Lock
    transition: ShareStateTransition

    buffer_lock: Lock
    first_index: int
    buffer: list[V]

    def get_buffer_item(self, index: int):
        with self.buffer_lock:
            return self.buffer[index - self.first_index]

    def pop_buffer(self, num: int):
        with self.buffer_lock:
            del self.buffer[:num]
            self.first_index += num
