from __future__ import annotations

from dataclasses import dataclass
from threading import RLock

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.flatmap.actions import FlatMapAction


@dataclass
class FlatMapSharedMemory:
    upstream_cancellable: Cancellable

    action: FlatMapAction
    lock: RLock
