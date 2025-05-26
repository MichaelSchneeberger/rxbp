from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from continuationmonad.typing import (
    DeferredHandler, ContinuationCertificate
)

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.share.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class ShareSharedMemory[V]:
    total_weight: int
    weight_partition: dict[int, int]

    init_certificate: ContinuationCertificate

    upstream_cancellation: Cancellable

    # modified synchronously
    deferred_handler: DeferredHandler

    # modified using lock
    lock: Lock
    transition: ShareStateTransition

    buffer_lock: Lock
    first_index: int
    buffer: list[V]

    def get_buffer_item(self, index: int):
        with self.buffer_lock:
            rel_index = index - self.first_index
            assert 0 <= rel_index <= len(self.buffer), f'No item in buffer at {index-self.first_index=}.'
            return self.buffer[index - self.first_index]

    def pop_buffer(self, num: int):
        with self.buffer_lock:
            del self.buffer[:num]
            self.first_index += num
