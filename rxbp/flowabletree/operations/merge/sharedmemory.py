from __future__ import annotations

from threading import Lock

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.merge.statetransitions import CancelTransition, MergeStateTransition


@dataclassabc(frozen=False)
class MergeSharedMemory(Cancellable):
    downstream: Observer
    transition: MergeStateTransition
    lock: Lock
    cancellables: dict[int, Cancellable]

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        state = transition.get_state()

        for id, certificate in state.certificates.items():
            self.cancellables[id].cancel(certificate)
