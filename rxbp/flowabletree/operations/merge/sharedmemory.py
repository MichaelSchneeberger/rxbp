from __future__ import annotations

from threading import Lock

from dataclassabc import dataclassabc

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.statetransitions import MergeStateTransition


@dataclassabc(frozen=False)
class MergeSharedMemory:
    downstream: Observer
    transition: MergeStateTransition
    lock: Lock
    cancellables: dict[int, Cancellable]
