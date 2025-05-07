from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.concatmap.statetransitions import ConcatMapStateTransition


@dataclass
class ConcatMapSharedMemory:
    upstream_cancellable: Cancellable

    transition: ConcatMapStateTransition
    lock: Lock
