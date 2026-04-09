from __future__ import annotations

from abc import ABC
from collections import deque
from threading import Lock

from dataclasses import dataclass

from contcert.typing import ContCert
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.statetransitions import MergeStateTransition


@dataclass(frozen=True)
class MergeItems[U](ABC): ...


@dataclass(frozen=True)
class OnNext[U](MergeItems[U]):
    item: U


@dataclass(frozen=True)
class OnNextAndComplete[U](MergeItems[U]):
    item: U


@dataclass(frozen=True)
class OnCompleted[U](MergeItems[U]): ...


@dataclass(frozen=False)
class MergeSharedMemory[U]:
    downstream: Observer[U]
    cancellables: dict[int, Cancellable]

    transition: MergeStateTransition
    transition_lock: Lock

    # there is always one certificate too few
    certs: list[ContCert]
    certs_lock: Lock

    items: deque[MergeItems[U]]
    upstream: deque[tuple[int, RxSingleObserver[None]]]
