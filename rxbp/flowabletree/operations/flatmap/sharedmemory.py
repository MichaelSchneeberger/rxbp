from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.flatmap.statetransitions import FlatMapStateTransition


@dataclass
class FlatMapSharedMemory:
    upstream_cancellable: Cancellable

    transition: FlatMapStateTransition
    lock: Lock
