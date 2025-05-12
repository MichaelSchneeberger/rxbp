from __future__ import annotations


from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.statetransitions import ZipStateTransition


@dataclassabc
class ZipSharedMemory[A, U]:
    transition: ZipStateTransition
    downstream: Observer[tuple[U, ...]]
    zip_func: Callable[[A, dict[int, U]], tuple[A, tuple[int, ...], U]]
    n_children: int
    cancellables: dict[int, Cancellable]
    lock: Lock
    acc: A

    # def cancel(self, certificate: ContinuationCertificate):
    #     transition = CancelTransition(
    #         child=None,  # type: ignore
    #         certificate=certificate,
    #         n_children=self.n_children,
    #     )
        
    #     with self.lock:
    #         transition.child = self.transition
    #         self.transition = transition

    #     match state := transition.get_state():
    #         case CancelledState(certificates=certificates):
    #             for id, certificate in certificates.items():
    #                 self.cancellables[id].cancel(certificate)

    #         case _:
    #             Exception(f"Unexpected state {state}.")
