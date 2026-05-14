from __future__ import annotations


from collections import deque
from threading import Lock
from typing import Callable, Never

from dataclasses import dataclass

from contcert.typing import ContCert
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.statetransitions import ZipStateTransition


@dataclass
class ZipSharedMemory[U, A, E]:
    downstream: Observer[A, E]
    zip_func: Callable[[dict[int, U], A], tuple[A, set[int]]]
    acc: A
    n_children: int
    cancellables: dict[int, Cancellable]
    transition: ZipStateTransition
    transition_lock: Lock

    # there is always one certificate too few
    certs: list[ContCert]
    certs_lock: Lock

    # concurrently add to the Deque
    items: deque[tuple[int, U]]
    upstream: deque[tuple[int, RxSingleObserver[None, Never]]]
