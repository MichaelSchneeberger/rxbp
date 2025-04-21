from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable, CancellationState
from rxbp.flowabletree.operations.merge.transitions import CancelTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.states import UpstreamID


@dataclass
class MergeCancellable(CancellationState):
    _certificate: ContinuationCertificate
    n_children: int
    cancellables: dict[UpstreamID, Cancellable]
    shared: MergeSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
        action = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
            n_children=self.n_children,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        state = action.get_state()

        for id, certificate in state.certificates.items():
            self.cancellables[id].cancel(certificate)
