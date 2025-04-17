from __future__ import annotations


from threading import RLock
from typing import Callable

from dataclassabc import dataclassabc

from rxbp.utils.lockmixin import LockMixin
from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.actions import ZipAction


@dataclassabc
class ZipSharedMemory[V](LockMixin):
    action: ZipAction
    downstream: Observer[tuple[V, ...]]
    zip_func: Callable[[dict[int, V]], tuple[int, ...]]
    n_children: int
    cancellables: tuple[Cancellable, ...]
    lock: RLock
