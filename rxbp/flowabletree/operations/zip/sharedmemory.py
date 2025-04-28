from __future__ import annotations


from threading import Lock
from typing import Callable

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.statetransitions import ZipStateTransition
from rxbp.flowabletree.operations.zip.states import CancelledState
from rxbp.flowabletree.operations.zip.statetransitions import CancelTransition


@dataclassabc
class ZipSharedMemory[V](Cancellable):
    transition: ZipStateTransition
    downstream: Observer[tuple[V, ...]]
    zip_func: Callable[[dict[int, V]], tuple[int, ...]]
    n_children: int
    cancellables: dict[int, Cancellable]
    lock: Lock

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
            n_children=self.n_children,
        )
        
        with self.lock:
            transition.child = self.transition
            self.transition = transition

        match state := transition.get_state():
            case CancelledState(certificates=certificates):
                for id, certificate in certificates.items():
                    self.cancellables[id].cancel(certificate)

            case _:
                Exception(f"Unexpected state {state}.")
