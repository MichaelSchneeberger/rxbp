from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from logging import Logger
from threading import Lock
from typing import Never

from lambdatrace import StackTrace
from contcert.typing import AsyncIndexTranslation, ContCert
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class AsyncShareSharedMemory[U, E]:
    virtual_count_partition: dict[int, int]
    async_index_partition: dict[int, int]
    # virtual_count: int
    async_index: int

    upstream: RxSingleObserver[None, Never] | None
    cert: ContCert
    cancellable: Cancellable

    # there is always one certificate too few
    async_translation: AsyncIndexTranslation

    # modified using lock
    transition_lock: Lock
    transition: ShareStateTransition

    buffer_lock: Lock
    first_index: int
    buffer: deque[U]

    stacks: tuple[StackTrace, ...]
    lambda_trace_logger: Logger
