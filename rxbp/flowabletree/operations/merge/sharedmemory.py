from __future__ import annotations

from threading import RLock

from dataclassabc import dataclassabc

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.transitions import MergeTransition


@dataclassabc(frozen=False)
class MergeSharedMemory:
    downstream: Observer
    action: MergeTransition
    n_children: int
    lock: RLock
