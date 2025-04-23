from __future__ import annotations

from dataclasses import dataclass
from threading import RLock

from continuationmonad.typing import (
    DeferredObserver,
)

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.share.transitions import (
    ShareTransition,
)


@dataclass(frozen=False)
class ShareSharedMemory[V]:
    total_weight: int

    upstream_cancellation: Cancellable

    # modified synchronously
    deferred_observer: DeferredObserver

    # modified using lock
    transition: ShareTransition
    lock: RLock

    buffer_lock: RLock
    first_index: int
    buffer: list[V]

    def get_buffer_item(self, index: int):
        with self.buffer_lock:
            return self.buffer[index - self.first_index]

    def pop_buffer(self, num: int):
        with self.buffer_lock:
            del self.buffer[:num]
            self.first_index += num
