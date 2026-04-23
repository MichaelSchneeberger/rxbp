from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from logging import Logger
from threading import Lock

from contcert.typing import ContextTranslation, Trampoline
from lambdatrace import StackTrace
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    ShareStateTransition,
)


@dataclass(frozen=False)
class AsyncShareSharedMemory[U]:
    virtual_count_partition: dict[int, int]
    context_partition: dict[int, int]
    # virtual_count: int
    context: int

    upstream: RxSingleObserver[None]
    cancellable: Cancellable

    # there is always one certificate too few
    # cert_lock: Lock
    # cert: ContCert
    context_translation: ContextTranslation

    # modified using lock
    transition_lock: Lock
    transition: ShareStateTransition
    # trampoline: Trampoline

    buffer_lock: Lock
    first_index: int
    buffer: deque[U]

    stacks: tuple[StackTrace, ...]
    lambda_trace_logger: Logger

