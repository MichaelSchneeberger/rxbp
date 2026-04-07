from __future__ import annotations

from threading import Lock

from dataclasses import dataclass

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.flatmap.statetransitions import (
    FlatMapStateTransition,
)


@dataclass(frozen=False)
class FlatMapSharedMemory:
    downstream: Observer
    transition: FlatMapStateTransition
    lock: Lock
    cancellables: dict[int, Cancellable]
    upstream_cancellable: Cancellable
