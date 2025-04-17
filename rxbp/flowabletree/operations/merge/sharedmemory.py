from __future__ import annotations

from threading import RLock

from dataclassabc import dataclassabc

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.actions import MergeAction


@dataclassabc(frozen=False)
class MergeSharedMemory:
    downstream: Observer
    action: MergeAction
    n_children: int
    lock: RLock
