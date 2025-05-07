from __future__ import annotations

from threading import Lock

from dataclassabc import dataclassabc

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.statetransitions import (
    FlatMapStateTransition,
)


@dataclassabc(frozen=False)
class FlatMapSharedMemory:
    downstream: Observer
    transition: FlatMapStateTransition
    lock: Lock
    cancellables: dict[int, Cancellable]
