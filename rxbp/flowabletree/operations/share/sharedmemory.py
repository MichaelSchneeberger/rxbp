from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from logging import Logger
from threading import Lock


from contcert.typing import ContCert
from lambdatrace import StackTrace
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.share.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class ShareSharedMemory[U]:
    virtual_count_partition: dict[int, int]
    virtual_count: int
    context: int

    upstream: RxSingleObserver[None]
    cancellable: Cancellable

    # there is always one certificate too few
    cert_lock: Lock
    cert: ContCert

    # modified using lock
    transition_lock: Lock
    transition: ShareStateTransition

    buffer_lock: Lock
    first_index: int
    buffer: deque[U]

    stacks: tuple[StackTrace, ...]
    lambda_trace_logger: Logger

    # downstream: deque[tuple[int, RxSingleObserver[None]]]

    # total_virtual_count: int
    #
    # init_certificate: ContCert
    #
    # upstream_cancellation: Cancellable
    #
    # # modified synchronously
    # deferred_handler: DeferredHandler


    # def get_buffer_item(self, index: int):
    #     with self.buffer_lock:
    #         rel_index = index - self.first_index
    #         assert 0 <= rel_index <= len(self.buffer), f'No item in buffer at {index-self.first_index=}.'
    #         return self.buffer[index - self.first_index]
    #
    # def pop_buffer(self, num: int):
    #     with self.buffer_lock:
    #         del self.buffer[:num]
    #         self.first_index += num
