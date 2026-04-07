from __future__ import annotations

from threading import Lock

from dataclasses import dataclass

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.statetransitions import MergeStateTransition


@dataclass(frozen=False)
class MergeSharedMemory:
    downstream: Observer
    transition: MergeStateTransition
    lock: Lock
    cancellables: dict[int, Cancellable]
